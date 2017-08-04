
module BBQue
  class Consumer
    attr_accessor :queue_names, :global_name, :redis, :logger

    def before_fork
      # Nothing
    end

    def after_fork
      # Nothing
    end

    def fork?
      false
    end

    def fork_and_wait
      if fork?
        before_fork

        pid = Process.fork do
          after_fork

          yield
        end

        Process.wait(pid)
      else
        yield
      end
    end

    def work(job, queue_name)
      fork_and_wait do
        begin
          job.work
        rescue Timeout::Error, StandardError => e
          logger.error "Job #{job.inspect} on #{queue_name.inspect} failed"
          logger.error e
        end
      end
    end

    def initialize(queue_names, global_name:, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.queue_names = queue_names
      self.global_name = global_name
      self.redis = redis
      self.logger = logger
    end

    def run
      cleanup

      trap "QUIT" do
        @stopping = true
      end

      trap "USR2" do
        @stopping = true
      end

      until @stopping
        run_once
      end

      cleanup
    end

    def run_once(timeout: 30)
      queue_name = await_wakeup(timeout)
      value = dequeue_single(queue_name) if queue_name
      queue_name, value = dequeue_multi(queue_names) unless value

      return unless value

      json = JSON.parse(value)
      job = BBQue.serializer.load(json["job"])

      logger.info "Job #{job.inspect} on #{queue_name.inspect} started"

      work(job, queue_name)

      logger.info "Job #{job.inspect} on #{queue_name.inspect} finished"

      delete(queue_name, value, job_key: json["job_key"])
    rescue Redis::BaseError => e
      logger.error e

      sleep 5
    rescue => e
      logger.error e
    end

    private

    def await_wakeup(timeout)
      key, _ = redis.blpop(Array(queue_names).map { |queue_name| "queue:#{queue_name}:notify" }, timeout)

      return key.gsub(/^queue:|:notify$/, "") if key

      nil
    end

    def delete(queue_name, value, job_key:)
      @delete_script =<<-EOF
        local queue_name, value, job_key = ARGV[1], ARGV[2], ARGV[3]

        redis.call('zrem', 'queue:' .. queue_name, value)

        if job_key ~= '' then
          if redis.call('hincrby', 'queue:' .. queue_name .. ':limits', job_key, -1) <= 0 then
            redis.call('hdel', 'queue:' .. queue_name .. ':limits', job_key)
          end
        end
      EOF

      redis.eval(@delete_script, argv: [queue_name, value, job_key])
    end

    def dequeue_multi(queue_names)
      Array(queue_names).each do |queue_name|
        if job = dequeue_single(queue_name)
          return [queue_name, job]
        end
      end

      nil
    end

    def dequeue_single(queue_name)
      @dequeue_single_script =<<-EOF
        local queue_name, global_name, timestamp = ARGV[1], ARGV[2], ARGV[3]

        local job = redis.call('zrange', 'queue:' .. queue_name, 0, 0, 'withscores')
        local ret = nil

        if job[1] then
          local value, score = job[1], tonumber(job[2])

          local json = cjson.decode(value)
          json['dequeued_at'] = timestamp

          ret = cjson.encode(json)

          redis.call('zadd', 'queue:' .. queue_name .. ':processing:' .. global_name, score, ret)
          redis.call('zrem', 'queue:' .. queue_name, value)
        end

        return ret
      EOF

      redis.eval(@dequeue_single_script, argv: [queue_name, global_name, Time.now.utc.strftime("%F")])
    end

    def cleanup
      @cleanup_script =<<-EOF
        local queue_name, global_name = ARGV[1], ARGV[2]

        local jobs = redis.call('zrange', 'queue:' .. queue_name .. ':processing:' .. global_name, 0, 100, 'withscores')
        local count = 0

        while jobs[1] do
          local i = 1

          while jobs[i] do
            local value, score = jobs[i], tonumber(jobs[i + 1])

            redis.call('zadd', 'queue:' .. queue_name, score, value)
            redis.call('rpush', 'queue:' .. queue_name .. ':notify', '1')
            redis.call('zrem', 'queue:' .. queue_name .. ':processing:' .. global_name, value)

            count = count + 1

            i = i + 2
          end

          jobs = redis.call('zrange', 'queue:' .. queue_name .. ':processing:' .. global_name, 0, 100, 'withscores')
        end

        return count
      EOF

      Array(queue_names).each do |queue_name|
        redis.eval(@cleanup_script, argv: [queue_name, global_name])
      end
    end
  end
end

