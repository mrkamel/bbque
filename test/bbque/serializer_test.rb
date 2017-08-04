
require File.expand_path("../../test_helper", __FILE__)

class BBQue::SerializerTest < MiniTest::Test
  class Job
    attr_accessor :attribute

    def initialize(attribute)
      self.attribute = attribute
    end

    def ==(job)
      attribute == job.attribute
    end
  end
      
  def test_dump_and_load
    job1 = Job.new("attribute")
    job2 = Job.new("other")

    refute_equal job1, job2

    assert_equal job1, BBQue::Serializer.load(BBQue::Serializer.dump(job1))
    assert_equal job2, BBQue::Serializer.load(BBQue::Serializer.dump(job2))
  end
end

