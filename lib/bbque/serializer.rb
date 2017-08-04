
require "base64"

module BBQue
  module Serializer
    def self.dump(object)
      Marshal.dump(object)
    end

    def self.load(object)
      Marshal.load(object)
    end
  end
end

