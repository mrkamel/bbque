
require "bbque/version"
require "bbque/serializer"
require "bbque/producer"
require "bbque/consumer"
require "bbque/scheduler"
require "digest"
require "thread"
require "json"
require "logger"
require "redis"

module BBQue
  class EnqueueError < StandardError; end
  class JobLimitError < StandardError; end

  class << self
    attr_accessor :serializer
  end

  self.serializer = BBQue::Serializer
end

