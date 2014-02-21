# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'google_bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "google_bigquery"
  spec.version       = GoogleBigquery::VERSION
  spec.authors       = ["miguel michelson"]
  spec.email         = ["miguelmichelson@gmail.com"]
  spec.description   = "Handy abstraction for GoogleBigQuery"
  spec.summary       = "This library is build on top of google-api-client"
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"

  spec.add_runtime_dependency("google-api-client")

  spec.add_dependency "hash-deep-merge"
  spec.add_dependency "activesupport"
end
