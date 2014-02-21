require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "Project" do
  before(:all) do
    config_setup
    @auth = GoogleBigquery::Auth.new
    @auth.authorize
    @project = config_options["email"].match(/(\d*)/)[0]
  end
  before :each do 
    @name = "whoa#{Time.now.to_i}"
  end

  it ".list" do
    expect(
      GoogleBigquery::Project.list["projects"].class
    ).to be Array
  end

end
