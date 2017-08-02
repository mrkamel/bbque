
require File.expand_path("../../test_helper", __FILE__)

class BBQueue::SchedulerTest < BBQueue::TestCase
  class Job; end

  def test_schedule
    producer = BBQueue::Producer.new("queue_name")

    producer.enqueue Job.new, delay: 60

    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")
    assert_equal 1, producer.redis.zcard("bbqueue:scheduler")

    scheduler = BBQueue::Scheduler.new

    scheduler.schedule

    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")
    assert_equal 1, producer.redis.zcard("bbqueue:scheduler")

    scheduler.schedule(Time.now.to_i + 61)

    assert_equal 1, producer.redis.llen("queue:queue_name:notify")
    assert_equal 1, producer.redis.zcard("queue:queue_name")
    assert_equal 0, producer.redis.zcard("bbqueue:scheduler")
  end
end
 
