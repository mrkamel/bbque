
module BBQueue
  class Producer
    attr_accessor :queue_name, :redis, :logger

    def initialize(queue_name, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.redis = redis
      self.queue_name = queue_name
      self.logger = logger
    end

    def enqueue(object)
      logger.info "Enqueue #{object.inspect} on #{queue_name.inspect}"

      obj = BBQueue::Serializer.dump(object)

      begin
        redis.rpush("queue:#{queue_name}", obj)
      rescue Redis::BaseError => e
        logger.error "Enqueue #{obj.inspect} on #{queue_name.inspect} failed"
        logger.error e

        return false
      end

      true
    end
  end
end

