
module BBQueue
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
      job = BBQueue::Serializer.load(json["job"])

      logger.info "Job #{job.inspect} on #{queue_name.inspect} started"

      work(job, queue_name)

      logger.info "Job #{job.inspect} on #{queue_name.inspect} finished"

      delete(queue_name, value)
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

    def delete(queue_name, value)
      @delete_script =<<-EOF
        redis.call('zrem', 'queue:' .. ARGV[1], ARGV[2])

        local json = cjson.decode(ARGV[2])

        if json['unique_key'] then
          redis.call('srem', 'queue:' .. ARGV[1] .. ':unique', json['unique_key'])
        end
      EOF

      redis.eval(@delete_script, argv: [queue_name, value])
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
        local job = redis.call('zrange', 'queue:' .. ARGV[1], 0, 0, 'withscores')
        local ret = nil

        if job[1] then
          local json = cjson.decode(job[1])
          json['dequeued_at'] = ARGV[3]

          ret = cjson.encode(json)

          redis.call('zadd', 'queue:' .. ARGV[1] .. ':processing:' .. ARGV[2], job[2], ret)
          redis.call('zrem', 'queue:' .. ARGV[1], job[1])
        end

        return ret
      EOF

      redis.eval(@dequeue_single_script, argv: [queue_name, global_name, Time.now.utc.strftime("%F")])
    end

    def cleanup
      @cleanup_script =<<-EOF
        local count = redis.call('zcard', 'queue:' .. ARGV[1] .. ':processing:' .. ARGV[2])
        local jobs = redis.call('zrange', 'queue:' .. ARGV[1] .. ':processing:' .. ARGV[2], 0, 100, 'withscores')

        while jobs[1] do
          local i = 1

          while jobs[i] do
            redis.call('zadd', 'queue:' .. ARGV[1], jobs[i + 1], jobs[i])
            redis.call('rpush', 'queue:' .. ARGV[1] .. ':notify', '1')
            redis.call('zrem', 'queue:' .. ARGV[1] .. ':processing:' .. ARGV[2], jobs[i])

            i = i + 2
          end

          jobs = redis.call('zrange', 'queue:' .. ARGV[1] .. ':processing:' .. ARGV[2], 0, 100, 'withscores')
        end

        return count
      EOF

      Array(queue_names).each do |queue_name|
        redis.eval(@cleanup_script, argv: [queue_name, global_name])
      end
    end
  end
end

