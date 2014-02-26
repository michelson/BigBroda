$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'rspec'
require 'debugger'
require File.join(File.dirname(__FILE__), '../lib', 'google_bigquery')
require 'stringio'
require "pry"
require "certified"

require 'vcr'


RSpec.configure do |config|

  #config.treat_symbols_as_metadata_keys_with_true_values = true

  VCR.configure do |c|
    c.cassette_library_dir = 'spec/fixtures/vcr_cassettes'
    c.hook_into :webmock
    c.allow_http_connections_when_no_cassette = true
    c.configure_rspec_metadata!
  end


  def fixture_key(type, filename)
    dir_name = type.to_s + "s"
    File.dirname(__FILE__) + "/fixtures/#{dir_name}/#{filename}"
  end

  def config_options
    config = YAML.load( File.open(fixture_key("config", "account_config.yml")) )
    config["key_file"]  = fixture_key("key", config["pem"])
    return config
  end

  def config_setup
    GoogleBigquery::Config.setup do |config|
      config.pass_phrase = config_options["pass_phrase"]
      config.key_file    = config_options["key_file"]
      config.client_id   = config_options["client_id"]
      config.scope       = config_options["scope"]
      config.profile_id  = config_options["profile_id"]
      config.email       =  config_options["email"]
    end
    GoogleBigquery::Config
    @project = config_options["options"]
  end

end
