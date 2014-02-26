require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "Project", :vcr => { :allow_unused_http_interactions => true } do
  
  before(:all) do
    VCR.use_cassette("Project/authorize_config") do
      config_setup
      @auth = GoogleBigquery::Auth.new
      @auth.authorize
      @project = config_options["email"].match(/(\d*)/)[0]
    end
  end

  before :each do 
    @name = "rspec_schema"
  end

  it ".list", :vcr do
    expect(
      GoogleBigquery::Project.list["projects"].class
    ).to be Array
  end

end
