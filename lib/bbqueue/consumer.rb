
module BBQueue
  class Consumer
    attr_accessor :queue_names, :redis, :logger

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

    def initialize(queue_names, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.queue_names = queue_names
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
        run_once
      end
    end

    def run_once
      keys = Array(queue_names).map { |queue_name| "queue:#{queue_name}" }

      key, value = redis.blpop(keys, 5)

      return unless value

      queue_name = key.gsub(/^[^:]+:/, "")
      job = BBQueue::Serializer.load(value)

      logger.info "Job #{job.inspect} on #{queue_name.inspect} started"

      work(job, queue_name)

      logger.info "Job #{job.inspect} on #{queue_name.inspect} finished"
    rescue Redis::BaseError => e
      logger.error e

      sleep 5
    rescue => e
      logger.error e
    end
  end
end

