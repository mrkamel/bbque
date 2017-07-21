
require File.expand_path("../../test_helper", __FILE__)

class BBQueue::ProducerTest < BBQueue::TestCase
  class Job
    attr_accessor :attribute

    def initialize(attribute)
      self.attribute = attribute
    end

    def work; end
  end

  def test_initialize
    BBQueue::Producer.new("queue", redis: Redis.new, logger: Logger.new("/dev/null"))
  end

  def test_enqueue
    job = Job.new("attribute")

    producer = BBQueue::Producer.new("queue_name")

    assert_equal 0, producer.redis.llen("queue:queue_name")
    assert_equal 0, producer.redis.zscan_each("queue:queue_name").to_a.size

    producer.enqueue job

    assert_equal 1, producer.redis.llen("queue:queue_name:notify")
    assert_equal 1, producer.redis.zscan_each("queue:queue_name").to_a.size
  end
end

