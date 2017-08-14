
require File.expand_path("../../test_helper", __FILE__)

class BBQue::ConsumerTest < BBQue::TestCase
  class Job
    attr_accessor :attribute

    def initialize(attribute)
      self.attribute = attribute
    end

    def work
      redis.rpush("results", attribute)
    end

    def redis
      @redis ||= Redis.new
    end
  end

  class ForkingConsumer < BBQue::Consumer
    def fork?
      true
    end

    def before_fork
      redis.rpush("results", "before_fork")
    end

    def after_fork
      redis.rpush("results", "after_fork")
    end
  end

  def test_run
    redis = Redis.new

    producer = BBQue::Producer.new("queue", redis: redis)
    consumer = BBQue::Consumer.new("queue", redis: redis, global_name: "consumer")

    producer.enqueue(Job.new("job1"))
    consumer.run_once

    producer.enqueue(Job.new("job2"))
    consumer.run_once

    assert_equal ["job1", "job2"], redis.lrange("results", 0, -1)
  end

  def test_run_empty
    redis = Redis.new

    BBQue::Consumer.new("queue", redis: redis, global_name: "consumer").run_once(timeout: 1)

    assert_equal [], redis.lrange("results", 0, -1)
  end

  def test_run_forking
    redis = Redis.new

    producer = BBQue::Producer.new("queue", redis: redis)
    consumer = ForkingConsumer.new("queue", redis: redis, global_name: "consumer")

    producer.enqueue(Job.new("job1"))
    consumer.run_once

    producer.enqueue(Job.new("job2"))
    consumer.run_once

    assert_equal ["before_fork", "after_fork", "job1", "before_fork", "after_fork", "job2"], redis.lrange("results", 0, -1)
  end

  def test_retry
    redis = Redis.new

    producer = BBQue::Producer.new("queue_name", redis: redis)
    consumer = BBQue::Consumer.new("queue_name", redis: redis, global_name: "consumer")

    producer.enqueue(Job.new("job1"))

    job_id = SecureRandom.hex

    redis.lpush("queue:queue_name:retry", job_id)
    redis.hset("queue:queue_name:jobs", job_id, JSON.dump(job: BBQue::Serializer.dump(Job.new("job2"))))
    redis.lpush("queue:queue_name:notify", "1")

    consumer.run_once
    consumer.run_once

    assert_equal 0, redis.llen("queue:queue_name:retry")
    assert_equal 0, redis.llen("queue_queue_name:notify")

    assert_equal ["job2", "job1"], redis.lrange("results", 0, -1)
  end

  def test_cleanup
    redis = Redis.new

    producer = BBQue::Producer.new("queue_name", redis: redis)
    consumer = BBQue::Consumer.new("queue_name", redis: redis, global_name: "consumer")

    producer.enqueue Job.new("job")

    assert_equal 0, redis.zcard("queue:queue_name:processing:consumer")
    assert_equal 1, redis.llen("queue:queue_name:notify")

    assert consumer.send(:dequeue)

    assert_equal 1, redis.llen("queue:queue_name:processing:consumer")
    assert_equal 1, redis.llen("queue:queue_name:notify")

    redis.lpush "queue:queue_name:notifications:consumer", "1"

    consumer.send(:cleanup)

    assert_equal 0, redis.llen("queue:queue_name:notifications:consumer")
    assert_equal 0, redis.llen("queue:queue_name:processing:consumer")
    assert_equal 2, redis.llen("queue:queue_name:notify")
    assert_equal 1, redis.llen("queue:queue_name:retry")
  end

  def test_delete
    redis = Redis.new

    producer = BBQue::Producer.new("queue_name", redis: redis)
    consumer = BBQue::Consumer.new("queue_name", redis: redis, global_name: "consumer")

    job_id = producer.enqueue(Job.new("job"), job_key: "job_key", limit: 1)

    assert redis.hexists("queue:queue_name:jobs", job_id)
    assert redis.zscore("queue:queue_name", job_id)
    assert_equal 1, redis.llen("queue:queue_name:notify")
    assert_equal "1", redis.hget("queue:queue_name:limits", "job_key")

    consumer.run_once

    refute redis.hexists("queue:queue_name:jobs", job_id)
    assert_nil redis.zscore("queue:queue_name", job_id)
    assert_equal 0, redis.llen("queue:queue_name:notify")
    assert_equal 0, redis.zcard("queue:queue_name:processing:consumer")
    assert_equal 0, redis.zcard("queue:queue_name:notifications:consumer")
    assert_nil redis.hget("queue:queue_name:limits", "job_key")
  end
end

