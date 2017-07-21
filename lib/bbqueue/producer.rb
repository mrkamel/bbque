
module BBQueue
  class Producer
    class InvalidPriority < StandardError; end
    class EnqueueError < StandardError; end

    attr_accessor :queue_name, :redis, :logger

    def initialize(queue_name, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.redis = redis
      self.queue_name = queue_name
      self.logger = logger
    end

    def enqueue(object, pri: 0)
      logger.info "Enqueue #{object.inspect} on #{queue_name.inspect}"

      raise(InvalidPriority, "Invalid priority, must be between -64 and 64") unless pri.between?(-64, 64)

      obj = BBQueue::Serializer.dump(object)
      score = ("%2i%014i" % [pri, (Time.now.to_f * 100).to_i]).to_i

      begin
        redis.multi do
          redis.zadd("queue:#{queue_name}", score, JSON.generate(id: SecureRandom.hex, enqueued_at: Time.now.utc.strftime("%F"), job: obj))
          redis.rpush("queue:#{queue_name}:notify", "1")
        end
      rescue Redis::BaseError => e
        raise EnqueueError, e.message
      end

      true
    end
  end
end

