require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "Dataset" do
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
    expect(GoogleBigquery::Dataset.list(@project)["datasets"]).to_not be_empty
  end

  context "operations" do 
    after(:each) do 
      GoogleBigquery::Dataset.delete(@project, @name) 
    end

    it "create & .delete" do
      expect(
        GoogleBigquery::Dataset.create(@project, 
          {"datasetReference"=> { "datasetId" => @name }} )["id"]
        ).to include @name
      GoogleBigquery::Dataset.delete(@project, @name) 
    end

    it ".update & delete" do
      expect(
        GoogleBigquery::Dataset.create(@project, 
          {"datasetReference"=> { "datasetId" =>@name }} )["id"]
        ).to include @name
      
      expect(
        GoogleBigquery::Dataset.update(@project, @name,
          {"datasetReference"=> {
           "datasetId" =>@name }, 
          "description"=> "foobar"} )["description"]
        ).to include "foobar"

      GoogleBigquery::Dataset.delete(@project, @name) 

    end

    it ".patch & delete" do
      expect(
        GoogleBigquery::Dataset.create(@project, 
          {"datasetReference"=> { "datasetId" =>@name }} )["id"]
        ).to include @name
      
      expect(
        GoogleBigquery::Dataset.patch(@project, @name,
          {"datasetReference"=> {
           "datasetId" =>@name }, 
          "description"=> "foobar"} )["description"]
        ).to include "foobar"

      GoogleBigquery::Dataset.delete(@project, @name) 

    end

    it ".get & delete" do 
      expect(
        GoogleBigquery::Dataset.create(@project, 
          {"datasetReference"=> { "datasetId" =>@name }} )["id"]
      ).to include @name

      expect(
        GoogleBigquery::Dataset.get(@project, @name )["id"]
      ).to include @name

      GoogleBigquery::Dataset.delete(@project, @name) 

    end
  end


end
