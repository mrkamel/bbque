
require "base64"

module BBQue
  module Serializer
    def self.dump(object)
      Base64.encode64(Marshal.dump(object))
    end

    def self.load(object)
      Marshal.load(Base64.decode64(object))
    end
  end
end

