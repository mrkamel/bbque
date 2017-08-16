
module BBQue
  class Scheduler
    attr_accessor :interval, :redis, :logger

    def initialize(interval: 60, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.interval = interval
      self.redis = redis
      self.logger = logger
    end

    def run
      loop do
        schedule

        sleep interval
      end
    end

    def list
      return enum_for(:list) unless block_given?

      redis.zscan_each("bbque:scheduler").each_slice(100) do |slice|
        redis.hmget("bbque:scheduler:jobs", slice.map(&:first)).each do |json|
          wrapped_job = JSON.parse(json)
          job = JSON.parse(wrapped_job["value"])

          yield(
            queue: wrapped_job["queue"],
            job_id: job["job_id"],
            job_key: job["job_key"],
            pri: job["pri"],
            enqueued_at: job["enqueued_at"],
            delay: job["delay"],
            job: BBQue.serializer.load(job["job"])
          )
        end
      end
    end

    def schedule(time = Time.now.to_i)
      @schedule_script ||=<<-EOF
        local timestamp = tonumber(ARGV[1])
        local count = 0

        local job_ids_with_scores = redis.call('zrange', 'bbque:scheduler', 0, 100, 'withscores')

        while job_ids_with_scores[1] do
          local i = 1

          while job_ids_with_scores[i] do
            local job_id = job_ids_with_scores[i]

            local value = redis.call('hget', 'bbque:scheduler:jobs', job_id)
            local score = tonumber(job_ids_with_scores[i + 1])

            if score <= timestamp then
              local job = cjson.decode(value)

              redis.call('zadd', 'queue:' .. job['queue'], tonumber(string.format('%i%013i', tonumber(job['pri']), redis.call('zcard', 'queue:' .. job['queue']))), job_id)
              redis.call('hset', 'queue:' .. job['queue'] .. ':jobs', job_id, job['value'])
              redis.call('rpush', 'queue:' .. job['queue'] .. ':notify', '1')
              redis.call('zrem', 'bbque:scheduler', job_id)
              redis.call('hdel', 'bbque:scheduler:jobs', job_id)

              i = i + 2

              count = count + 1
            else
              return count
            end
          end

          job_ids_with_scores = redis.call('zrange', 'bbque:scheduler', 0, 100, 'withscores')
        end

        return count
      EOF

      redis.eval(@schedule_script, argv: [time])
    end
  end
end

