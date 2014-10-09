require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "Config class" do
  before(:all) do

    GoogleBigquery::Config.setup do |config|
      config.pass_phrase = config_options["pass_phrase"]
      config.key_file    = config_options["key_file"]
      config.scope       = config_options["scope"]
      config.email       =  config_options["email"]
    end
  end

  it "has all the keys required" do
    GoogleBigquery::Config.pass_phrase.should_not be_empty
    GoogleBigquery::Config.key_file.should_not be_empty
    GoogleBigquery::Config.scope.should_not be_empty
    GoogleBigquery::Config.email.should_not be_empty
  end

end
