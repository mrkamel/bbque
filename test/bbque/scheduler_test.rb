
require File.expand_path("../../test_helper", __FILE__)

class BBQue::SchedulerTest < BBQue::TestCase
  class Job; end

  def test_schedule
    producer = BBQue::Producer.new("queue_name")

    producer.enqueue Job.new, delay: 60

    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")
    assert_equal 1, producer.redis.zcard("bbque:scheduler")
    assert_equal 1, producer.redis.hlen("bbque:scheduler:jobs")

    scheduler = BBQue::Scheduler.new

    scheduler.schedule

    assert_equal 0, producer.redis.llen("queue:queue_name:notify")
    assert_equal 0, producer.redis.zcard("queue:queue_name")
    assert_equal 1, producer.redis.zcard("bbque:scheduler")
    assert_equal 1, producer.redis.hlen("bbque:scheduler:jobs")

    scheduler.schedule(Time.now.to_i + 61)

    assert_equal 1, producer.redis.llen("queue:queue_name:notify")
    assert_equal 1, producer.redis.zcard("queue:queue_name")
    assert_equal 0, producer.redis.zcard("bbque:scheduler")
    assert_equal 0, producer.redis.hlen("bbque:scheduler:jobs")
  end
end
 
