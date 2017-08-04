
require "base64"

module BBQue
  module Serializer
    def self.dump(object)
      Base64.strict_encode64 Marshal.dump(object)
    end

    def self.load(object)
      Marshal.load Base64.strict_decode64(object)
    end
  end
end

