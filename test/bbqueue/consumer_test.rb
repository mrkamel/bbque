
require File.expand_path("../../test_helper", __FILE__)

class BBQueue::ConsumerTest < BBQueue::TestCase
  class Job
    attr_accessor :attribute, :failing

    def initialize(attribute, failing: false)
      self.attribute = attribute
      self.failing = failing
    end

    def work
      redis.rpush("results", attribute)

      raise if failing
    end

    def redis
      @redis ||= Redis.new
    end
  end

  class ForkingConsumer < BBQueue::Consumer
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

    producer1 = BBQueue::Producer.new("queue1", redis: redis)
    producer2 = BBQueue::Producer.new("queue2", redis: redis)

    consumer = BBQueue::Consumer.new(["queue1", "queue2"], global_name: "consumer")

    producer1.enqueue(Job.new("job1"))
    consumer.run_once

    producer2.enqueue(Job.new("job2"))
    consumer.run_once

    assert_equal ["job1", "job2"], redis.lrange("results", 0, -1)
  end

  def test_run_without_notification
    redis = Redis.new

    producer1 = BBQueue::Producer.new("queue1", redis: redis)
    producer2 = BBQueue::Producer.new("queue2", redis: redis)

    consumer = BBQueue::Consumer.new(["queue1", "queue2"], global_name: "consumer")

    producer1.enqueue(Job.new("job1"))
    redis.del "queue:queue1:notify"
    consumer.run_once(timeout: 1)

    producer2.enqueue(Job.new("job2"))
    redis.del "queue:queue2:notify"
    consumer.run_once(timeout: 1)

    assert_equal ["job1", "job2"], redis.lrange("results", 0, -1)
  end

  def test_run_empty
    redis = Redis.new

    BBQueue::Consumer.new("queue", global_name: "consumer").run_once(timeout: 1)

    assert_equal [], redis.lrange("results", 0, -1)
  end

  def test_run_failing
    redis = Redis.new

    producer1 = BBQueue::Producer.new("queue1", redis: redis)
    producer2 = BBQueue::Producer.new("queue2", redis: redis)

    consumer = BBQueue::Consumer.new(["queue1", "queue2"], global_name: "consumer")

    producer1.enqueue(Job.new("job1", failing: true))
    consumer.run_once

    producer2.enqueue(Job.new("job2", failing: true))
    consumer.run_once

    assert_equal ["job1", "job2"], redis.lrange("results", 0, -1)
  end

  def test_run_forking
    redis = Redis.new

    producer1 = BBQueue::Producer.new("queue1", redis: redis)
    producer2 = BBQueue::Producer.new("queue2", redis: redis)

    consumer = ForkingConsumer.new(["queue1", "queue2"], global_name: "consumer")

    producer1.enqueue(Job.new("job1"))
    consumer.run_once

    producer2.enqueue(Job.new("job2"))
    consumer.run_once

    assert_equal ["before_fork", "after_fork", "job1", "before_fork", "after_fork", "job2"], redis.lrange("results", 0, -1)
  end

  def test_cleanup
    redis = Redis.new

    producer = BBQueue::Producer.new("queue_name", redis: redis)
    consumer = BBQueue::Consumer.new("queue_name", global_name: "consumer")

    producer.enqueue Job.new("job")

    assert_equal 0, redis.zcard("queue:queue_name:processing:consumer")
    assert_equal 1, redis.llen("queue:queue_name:notify")
    assert_equal 1, redis.zcard("queue:queue_name")

    consumer.send(:dequeue_single, "queue_name")

    assert_equal 1, redis.zcard("queue:queue_name:processing:consumer")
    assert_equal 1, redis.llen("queue:queue_name:notify")
    assert_equal 0, redis.zcard("queue:queue_name")

    consumer.send(:cleanup)

    assert_equal 0, redis.zcard("queue:queue_name:processing:consumer")
    assert_equal 2, redis.llen("queue:queue_name:notify")
    assert_equal 1, redis.zcard("queue:queue_name")
  end

  def test_delete
    redis = Redis.new

    producer = BBQueue::Producer.new("queue_name", redis: redis)
    consumer = BBQueue::Consumer.new("queue_name", global_name: "consumer")

    producer.enqueue Job.new("job"), job_key: "job_key", limit: 1

    assert_equal 1, redis.llen("queue:queue_name:notify")
    assert_equal 1, redis.zcard("queue:queue_name")
    assert_equal "1", redis.hget("queue:queue_name:limits", "job_key")

    consumer.run_once

    assert_equal 0, redis.llen("queue:queue_name:notify")
    assert_equal 0, redis.zcard("queue:queue_name")
    assert_nil redis.hget("queue:queue_name:limits", "job_key")
  end
end

