
module BBQueue
  class Producer
    class InvalidPriority < StandardError; end
    class NotUniqueError < StandardError; end
    class EnqueueError < StandardError; end

    attr_accessor :queue_name, :redis, :logger

    def initialize(queue_name, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.redis = redis
      self.queue_name = queue_name
      self.logger = logger
    end

    def enqueue(object, pri: 0, unique_key: nil)
      logger.info "Enqueue #{object.inspect} on #{queue_name.inspect}"

      raise(InvalidPriority, "Invalid priority, must be between -64 and 64") unless pri.between?(-64, 64)

      serialized_object = BBQueue::Serializer.dump(object)
      score = ("%2i%014i" % [pri, (Time.now.to_f * 100).to_i]).to_i

      begin
        @enqueue_script ||=<<-EOF
          if ARGV[4] ~= '' then
            if redis.call('sismember', 'queue:' .. ARGV[1] .. ':unique', ARGV[4]) == 1 then
              return false
            else
              redis.call('sadd', 'queue:' .. ARGV[1] .. ':unique', ARGV[4])
            end
          end

          redis.call('zadd', 'queue:' .. ARGV[1], tonumber(ARGV[2]), ARGV[3])
          redis.call('rpush', 'queue:' .. ARGV[1] .. ':notify', '1')

          return true
        EOF

        value = {}
        value[:enqueued_at] = Time.now.utc.strftime("%F")
        value[:job] = serialized_object
        value[:unique_key] = unique_key if unique_key

        result = redis.eval(
          @enqueue_script,
          argv: [
            queue_name,
            score,
            JSON.generate(value),
            unique_key
          ]
        )

        raise(NotUniqueError) unless result
      rescue Redis::BaseError => e
        raise EnqueueError, e.message
      end

      true
    end
  end
end

