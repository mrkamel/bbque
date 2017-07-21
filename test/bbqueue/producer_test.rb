
require File.expand_path("../../test_helper", __FILE__)

class BBQueue::ProducerTest < BBQueue::TestCase
  class Job; end

  def test_initialize
    BBQueue::Producer.new("queue", redis: Redis.new, logger: Logger.new("/dev/null"))
  end

  def test_enqueue
    producer = BBQueue::Producer.new("queue_name")

    assert_equal 0, producer.redis.llen("queue:queue_name")
    assert_equal 0, producer.redis.zscan_each("queue:queue_name").to_a.size

    producer.enqueue Job.new

    assert_equal 1, producer.redis.llen("queue:queue_name:notify")
    assert_equal 1, producer.redis.zscan_each("queue:queue_name").to_a.size
  end

  def test_unique
    producer = BBQueue::Producer.new("queue_name")

    producer.enqueue Job.new, unique_key: "unique"

    assert_raises BBQueue::Producer::NotUniqueError do
      producer.enqueue Job.new, unique_key: "unique"
    end
  end
end

