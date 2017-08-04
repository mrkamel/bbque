
require "bbque"
require "minitest"
require "minitest/autorun"
require "redis"

class BBQue::TestCase < MiniTest::Test
  def setup
    Redis.new.flushdb
  end
end

