
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
      key, _ = redis.blpop(Array(queue_names).map { |queue_name| "queue:#{queue_name}:notify" }, timeout)
      value = dequeue_single(key) if key
      key, value = dequeue_multi(Array(queue_names).map { |queue_name| "queue:#{queue_name}" }) unless value

      return unless value

      queue_name = key.gsub(/^[^:]+:/, "")
      job = BBQueue::Serializer.load(JSON.parse(value)["job"])

      logger.info "Job #{job.inspect} on #{queue_name.inspect} started"

      work(job, queue_name)

      logger.info "Job #{job.inspect} on #{queue_name.inspect} finished"

      redis.zrem("#{key}:processing:#{global_name}", value)
    rescue Redis::BaseError => e
      logger.error e

      sleep 5
    rescue => e
      logger.error e
    end

    def dequeue_multi(queue_names)
      queue_names.each do |queue_name|
        if job = dequeue_single(queue_name)
          return [queue_name, job]
        end
      end

      nil
    end

    def dequeue_single(queue_name)
      @dequeue_single_script =<<-EOF
        local job = redis.call('zrange', ARGV[1], 0, 0, 'withscores')

        if job[1] then
          local json = cjson.decode(job[1])
          json['dequeued_at'] = ARGV[3]

          redis.call('zadd', ARGV[1] .. ':processing:' .. ARGV[2], job[2], cjson.encode(json))
          redis.call('zrem', ARGV[1], job[1])
        end

        return job[1]
      EOF

      redis.eval(@dequeue_single_script, argv: [queue_name, global_name, Time.now.utc.strftime("%F")])
    end

    def cleanup
      @cleanup_script =<<-EOF
        local count = redis.call('zcard', ARGV[1] .. ':processing:' .. ARGV[2])
        local jobs = redis.call('zrange', ARGV[1] .. ':processing:' .. ARGV[2], 0, 100, 'withscores')

        while jobs[1] do
          local i = 1

          while jobs[i] do
            redis.call('zadd', ARGV[1], jobs[i + 1], jobs[i])
            redis.call('zrem', ARGV[1] .. ':processing:' .. ARGV[2], jobs[i])

            i = i + 2
          end

          jobs = redis.call('zrange', ARGV[1] .. ':processing:' .. ARGV[2], 0, 100, 'withscores')
        end

        return count
      EOF

      redis.eval(@cleanup_script, argv: [queue_name, global_name])
    end
  end
end

