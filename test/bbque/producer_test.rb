
require File.expand_path("../../test_helper", __FILE__)

class BBQue::ProducerTest < BBQue::TestCase
  class Job; end

  def test_initialize
    BBQue::Producer.new("queue", logger: Logger.new("/dev/null"))
  end

  def test_enqueue
    producer = BBQue::Producer.new("queue_name")

    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.hlen("queue:queue_name:jobs")
    assert_equal 0, producer.redis.zcard("queue:queue_name")

    job_id = producer.enqueue(Job.new)

    assert producer.redis.hexists("queue:queue_name:jobs", job_id)
    assert producer.redis.zscore("queue:queue_name", job_id)

    assert_equal 1, producer.redis.llen("queue:queue_name:notify")
  end

  def test_enqueue_with_limit
    producer = BBQue::Producer.new("queue_name")

    assert_nil producer.redis.hget("queue:queue_name:limits", "job_key")

    producer.enqueue Job.new, job_key: "job_key", limit: 1

    assert_equal "1", producer.redis.hget("queue:queue_name:limits", "job_key")

    assert_raises BBQue::Producer::JobLimitError do
      producer.enqueue Job.new, job_key: "job_key", limit: 1
    end
  end

  def test_enqueue_with_delay
    producer = BBQue::Producer.new("queue_name")

    assert_equal 0, producer.redis.zcard("bbque:scheduler")
    assert_equal 0, producer.redis.hlen("bbque:scheduler:jobs")
    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.hlen("queue:queue_name:jobs")
    assert_equal 0, producer.redis.zcard("queue:queue_name")

    producer.enqueue Job.new, delay: 30

    assert_equal 1, producer.redis.zcard("bbque:scheduler")
    assert_equal 1, producer.redis.hlen("bbque:scheduler:jobs")
    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.hlen("queue:queue_name:jobs")
    assert_equal 0, producer.redis.zcard("queue:queue_name")
  end

  def test_list
    producer = BBQue::Producer.new("queue_name")

    producer.enqueue Job.new

    assert_equal 1, producer.list.count

    producer.enqueue Job.new

    assert_equal 2, producer.list.count
  end
end

