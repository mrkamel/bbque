# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'bbque/version'

Gem::Specification.new do |spec|
  spec.name          = "bbque"
  spec.version       = BBQue::VERSION
  spec.authors       = ["Benjamin Vetter"]
  spec.email         = ["vetter@plainpicture.de"]
  spec.summary       = %q{Queue and process ruby job objects in the background}
  spec.description   = %q{Queue and process ruby job objects in the background}
  spec.homepage      = "https://github.com/mrkamel/bbque"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"

  spec.add_dependency "redis", ">=4.6.0"
  spec.add_dependency "json"
end

