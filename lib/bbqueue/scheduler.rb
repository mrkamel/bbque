
module BBQueue
  class Scheduler
    attr_accessor :redis, :logger

    def initialize(redis: Redis.new, logger: Logger.new("/dev/null"))
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

        sleep 60
      end
    end

    def schedule(time = Time.now.to_i)
      @schedule_script ||=<<-EOF
        local timestamp = tonumber(ARGV[1])
        local count = 0

        local jobs = redis.call('zrange', 'bbqueue:scheduler', 0, 100, 'withscores')

        while jobs[1] do
          local i = 1

          while jobs[i] do
            local value = jobs[i]
            local score = tonumber(jobs[i + 1])

            if score <= timestamp then
              local job = cjson.decode(value)

              redis.call('zadd', 'queue:' .. job['queue'], tonumber(job['score']), job['value'])
              redis.call('rpush', 'queue:' .. job['queue'] .. ':notify', '1')
              redis.call('zrem', 'bbqueue:scheduler', value)

              i = i + 2

              count = count + 1
            else
              return count
            end
          end

          jobs = redis.call('zrange', 'bbqueue:scheduler', 0, 100, 'withscores')
        end

        return count
      EOF

      redis.eval(@schedule_script, argv: [time])
    end
  end
end

