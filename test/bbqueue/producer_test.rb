
require File.expand_path("../../test_helper", __FILE__)

class BBQueue::ProducerTest < BBQueue::TestCase
  class Job; end

  def test_initialize
    BBQueue::Producer.new("queue", redis: Redis.new, logger: Logger.new("/dev/null"))
  end

  def test_enqueue
    producer = BBQueue::Producer.new("queue_name")

    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")

    producer.enqueue Job.new

    assert_equal 1, producer.redis.llen("queue:queue_name:notify")
    assert_equal 1, producer.redis.zcard("queue:queue_name")
  end

  def test_enqueue_with_limit
    producer = BBQueue::Producer.new("queue_name")

    assert_nil producer.redis.hget("queue:queue_name:limits", "job_key")

    producer.enqueue Job.new, job_key: "job_key", limit: 1

    assert_equal "1", producer.redis.hget("queue:queue_name:limits", "job_key")

    assert_raises BBQueue::Producer::JobLimitError do
      producer.enqueue Job.new, job_key: "job_key", limit: 1
    end
  end

  def test_enqueue_with_delay
    producer = BBQueue::Producer.new("queue_name")

    assert_equal 0, producer.redis.zcard("bbqueue:scheduler")
    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")

    producer.enqueue Job.new, delay: 30

    assert_equal 1, producer.redis.zcard("bbqueue:scheduler")
    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")
  end
end

