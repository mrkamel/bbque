
require "bbqueue"
require "minitest"
require "minitest/autorun"
require "redis"

class BBQueue::TestCase < MiniTest::Test
  def setup
    Redis.new.flushdb
  end
end

