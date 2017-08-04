
module BBQue
  class Producer
    class JobLimitError < StandardError; end
    class EnqueueError < StandardError; end

    attr_accessor :queue_name, :redis, :logger

    def initialize(queue_name, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.redis = redis
      self.queue_name = queue_name
      self.logger = logger
    end

    def enqueue(object, pri: 0, job_key: nil, limit: nil, delay: nil)
      logger.info "Enqueue #{object.inspect} on #{queue_name.inspect}"

      raise(ArgumentError, "Invalid priority, must be between -64 and 64") unless pri.between?(-64, 64)
      raise(ArgumentError, "Limit must be a positive number") if limit && limit <= 0
      raise(ArgumentError, "You must specify a job key if limit is specified") if limit.to_i > 0 && job_key.nil?

      serialized_object = BBQue::Serializer.dump(object)
      score = ("%2i%014i" % [pri, (Time.now.to_f * 100).to_i]).to_i

      begin
        @enqueue_script ||=<<-EOF
          local queue_name, score, value, job_key, limit, delay = ARGV[1], tonumber(ARGV[2]), ARGV[3], ARGV[4], tonumber(ARGV[5]), tonumber(ARGV[6])

          if limit > 0 and job_key ~= '' then
            if redis.call('hincrby', 'queue:' .. queue_name .. ':limits', job_key, 1) > limit then
              redis.call('hincrby', 'queue:' .. queue_name .. ':limits', job_key, -1)

              return 'TOO MANY'
            end
          end

          if delay then
            redis.call('zadd', 'bbque:scheduler', delay, cjson.encode({ queue = queue_name, score = score, value = value }))
          else
            redis.call('zadd', 'queue:' .. queue_name, score, value)
            redis.call('rpush', 'queue:' .. queue_name .. ':notify', '1')
          end

          return true
        EOF

        value = {}
        value[:enqueued_at] = Time.now.utc.strftime("%F")
        value[:job_key] = job_key
        value[:job] = serialized_object

        result = redis.eval(
          @enqueue_script,
          argv: [
            queue_name,
            score,
            JSON.generate(value),
            job_key,
            limit || 0,
            delay.to_i > 0 ? Time.now.to_i + delay.to_i : nil
          ]
        )

        raise(JobLimitError) if result == "TOO MANY"
      rescue Redis::BaseError => e
        raise EnqueueError, e.message
      end

      true
    end
  end
end

