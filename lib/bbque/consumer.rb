
module BBQue
  class Consumer
    attr_accessor :queue_name, :global_name, :redis, :logger

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

    def work(job)
      fork_and_wait do
        begin
          job.work
        rescue Timeout::Error, StandardError => e
          logger.error "Job #{job.inspect} on #{queue_name.inspect} failed"
          logger.error e
        end
      end
    end

    def initialize(queue_name, global_name:, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.queue_name = queue_name
      self.global_name = global_name
      self.redis = redis
      self.logger = logger

      @stop_mutex = Mutex.new
      @stopping = false

      @wakeup_queue = Queue.new
    end

    def run
      cleanup

      trap "QUIT" do
        Thread.new { stop }
      end

      trap "USR2" do
        Thread.new { stop }
      end

      until stopping?
        run_once
      end

      cleanup
    end

    def run_once(timeout: 5)
      await_wakeup(timeout)

      return if stopping?

      value = dequeue

      return unless value

      json = JSON.parse(value)
      job = BBQue.serializer.load(json["job"])

      logger.info "Job #{job.inspect} on #{queue_name.inspect} started"

      work(job)

      logger.info "Job #{job.inspect} on #{queue_name.inspect} finished"

      delete(json["job_id"], job_key: json["job_key"])
    rescue Redis::BaseError => e
      logger.error e

      sleep 5
    rescue => e
      logger.error e
    end

    private

    def stop
      @stop_mutex.synchronize do
        @stopping = true
      end

      @wakeup_queue.enq(nil)
    end

    def stopping?
      @stop_mutex.synchronize do
        @stopping
      end
    end

    def await_wakeup(timeout)
      Thread.new do
        @wakeup_queue.enq redis.brpoplpush("queue:#{queue_name}:notify", "queue:#{queue_name}:notifications:#{global_name}", timeout)
      end

      @wakeup_queue.deq
    end

    def delete(job_id, job_key:)
      @delete_script =<<-EOF
        local queue_name, job_id, global_name, job_key = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

        redis.call('lrem', 'queue:' .. queue_name .. ':processing:' .. global_name, 1, job_id)
        redis.call('rpop', 'queue:' .. queue_name .. ':notifications:' .. global_name)
        redis.call('hdel', 'queue:' .. queue_name .. ':jobs', job_id)

        if job_key ~= '' then
          if redis.call('hincrby', 'queue:' .. queue_name .. ':limits', job_key, -1) <= 0 then
            redis.call('hdel', 'queue:' .. queue_name .. ':limits', job_key)
          end
        end
      EOF

      redis.eval(@delete_script, argv: [queue_name, job_id, global_name, job_key])
    end

    def dequeue
      @dequeue_script =<<-EOF
        local queue_name, global_name, timestamp = ARGV[1], ARGV[2], ARGV[3]
        local job_id, job = nil, nil

        job_id = redis.call('rpop', 'queue:' .. queue_name .. ':retry')

        if not job_id then
          local job_ids = redis.call('zrange', 'queue:' .. queue_name, 0, 0)

          if job_ids[1] then
            job_id = job_ids[1]
          end
        end

        if job_id then
          job = redis.call('hget', 'queue:' .. queue_name .. ':jobs', job_id)
        end

        if job then
          redis.call('lpush', 'queue:' .. queue_name .. ':processing:' .. global_name, job_id)
          redis.call('zrem', 'queue:' .. queue_name, job_id)
        end

        return job
      EOF

      redis.eval(@dequeue_script, argv: [queue_name, global_name, Time.now.utc.strftime("%F")])
    end

    def cleanup
      @cleanup_script =<<-EOF
        local queue_name, global_name = ARGV[1], ARGV[2]
        local count = 0

        local job_id = redis.call('rpop', 'queue:' .. queue_name .. ':processing:' .. global_name)

        while job_id do
          redis.call('lpush', 'queue:' .. queue_name .. ':retry', job_id)

          count = count + 1

          job_id = redis.call('rpop', 'queue:' .. queue_name .. ':processing:' .. global_name)
        end

        local notification = redis.call('rpop', 'queue:' .. queue_name .. ':notifications:' .. global_name)

        while notification do
          redis.call('lpush', 'queue:' .. queue_name .. ':notify', notification)

          notification = redis.call('rpop', 'queue:' .. queue_name .. ':notifications:' .. global_name)
        end

        return count
      EOF

      redis.eval(@cleanup_script, argv: [queue_name, global_name])
    rescue Redis::BaseError => e
      logger.error e

      sleep 5

      retry
    end
  end
end

