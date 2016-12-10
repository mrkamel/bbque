
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
    producer.enqueue job

    assert_equal BBQueue::Serializer.dump(job), producer.redis.rpop("queue:queue_name")
  end
end

