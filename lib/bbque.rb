
require "bbque/version"
require "bbque/serializer"
require "bbque/producer"
require "bbque/consumer"
require "bbque/scheduler"
require "securerandom"
require "json"
require "logger"
require "redis"

module BBQue
  class << self
    attr_accessor :serializer
  end

  self.serializer = BBQue::Serializer
end

