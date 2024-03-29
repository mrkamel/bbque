
module BBQue
  class Producer
    attr_accessor :queue_name, :redis, :logger

    def initialize(queue_name, redis: Redis.new, logger: Logger.new("/dev/null"))
      self.redis = redis
      self.queue_name = queue_name
      self.logger = logger
    end

    def self.generate_job_id
      Digest::SHA256.hexdigest("#{rand}:#{Time.now.to_f}")[0 .. 31]
    end

    def list
      return enum_for(:list) unless block_given?

      redis.zscan_each("queue:#{queue_name}").each_slice(100) do |slice|
        redis.hmget("queue:#{queue_name}:jobs", slice.map(&:first)).each do |json|
          job = JSON.parse(json)

          yield(
            job_id: job["job_id"],
            job_key: job["job_key"],
            pri: job["pri"],
            enqueued_at: job["enqueued_at"],
            job: BBQue.serializer.load(job["job"])
          )
        end
      end
    end

    def size
      redis.zcard("queue:#{queue_name}")
    end

    def enqueue(object, pri: 0, job_key: nil, limit: nil, delay: nil)
      logger.info "Enqueue #{object.inspect} on #{queue_name.inspect}"

      raise(ArgumentError, "Invalid priority, must be between -512 and 512") unless pri.between?(-512, 512)
      raise(ArgumentError, "Limit must be a positive number") if limit && limit <= 0
      raise(ArgumentError, "You must specify a job key if limit is specified") if limit.to_i > 0 && job_key.nil?

      serialized_object = BBQue.serializer.dump(object)

      begin
        @enqueue_script ||=<<-EOF
          local queue_name, pri, value, job_id, job_key, limit, delay_timestamp = ARGV[1], tonumber(ARGV[2]), ARGV[3], ARGV[4], ARGV[5], tonumber(ARGV[6]), tonumber(ARGV[7])

          if limit > 0 and job_key ~= '' then
            if redis.call('hincrby', 'queue:' .. queue_name .. ':limits', job_key, 1) > limit then
              redis.call('hincrby', 'queue:' .. queue_name .. ':limits', job_key, -1)

              return 'TOO MANY'
            end
          end

          if delay_timestamp > 0 then
            redis.call('hset', 'bbque:scheduler:jobs', job_id, cjson.encode({ queue = queue_name, pri = pri, job_id = job_id, value = value }))
            redis.call('zadd', 'bbque:scheduler', delay_timestamp, job_id)
          else
            redis.call('zadd', 'queue:' .. queue_name, tonumber(string.format('%i%013i', 0 - pri, redis.call('zcard', 'queue:' .. queue_name))), job_id)
            redis.call('hset', 'queue:' .. queue_name .. ':jobs', job_id, value)
            redis.call('lpush', 'queue:' .. queue_name .. ':notify', '1')
          end

          return true
        EOF

        job_id = self.class.generate_job_id

        value = {}
        value[:enqueued_at] = Time.now.utc.strftime("%F")
        value[:job_key] = job_key
        value[:job_id] = job_id
        value[:pri] = pri
        value[:delay] = delay if delay
        value[:job] = serialized_object

        result = redis.eval(
          @enqueue_script,
          argv: [
            queue_name,
            pri,
            JSON.generate(value),
            job_id,
            job_key.to_s,
            limit || 0,
            delay.to_i > 0 ? Time.now.to_i + delay.to_i : 0
          ]
        )

        raise(BBQue::JobLimitError) if result == "TOO MANY"
      rescue Redis::BaseError => e
        raise BBQue::EnqueueError, e.message
      end

      job_id
    end
  end
end

