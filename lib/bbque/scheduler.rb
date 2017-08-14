
module BBQue
  class Scheduler
    attr_accessor :interval, :redis, :logger

    def initialize(interval: 60, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.interval = interval
      self.redis = redis
      self.logger = logger
    end

    def run
      trap "QUIT" do
        @stopping = true
      end

      trap "USR2" do
        @stopping = true
      end

      until @stopping
        schedule

        sleep interval
      end
    end

    def schedule(time = Time.now.to_i)
      @schedule_script ||=<<-EOF
        local timestamp = tonumber(ARGV[1])
        local count = 0

        local jobs = redis.call('zrange', 'bbque:scheduler', 0, 100, 'withscores')

        while jobs[1] do
          local i = 1

          while jobs[i] do
            local value = jobs[i]
            local score = tonumber(jobs[i + 1])

            if score <= timestamp then
              local job = cjson.decode(value)

              redis.call('zadd', 'queue:' .. job['queue'], tonumber(string.format('%i%013i', tonumber(job['pri']), redis.call('zcard', 'queue:' .. job['queue']))), job['job_id'])
              redis.call('hset', 'queue:' .. job['queue'] .. ':jobs', job['job_id'], job['value'])
              redis.call('rpush', 'queue:' .. job['queue'] .. ':notify', '1')
              redis.call('zrem', 'bbque:scheduler', value)

              i = i + 2

              count = count + 1
            else
              return count
            end
          end

          jobs = redis.call('zrange', 'bbque:scheduler', 0, 100, 'withscores')
        end

        return count
      EOF

      redis.eval(@schedule_script, argv: [time])
    end
  end
end

