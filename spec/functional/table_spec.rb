

require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "Dataset" do
  before(:all) do
    config_setup
    @auth = GoogleBigquery::Auth.new
    @auth.authorize
    @project = config_options["email"].match(/(\d*)/)[0]
  end

  context "operations" do 
    before :each do 
      @name = "whoa#{Time.now.to_i}"
      GoogleBigquery::Dataset.create(@project, {"datasetReference"=> { "datasetId" => @name }} )["id"]  
    end

    after(:each) do 
      GoogleBigquery::Dataset.delete(@project, @name) 
    end

    it ".list" do
      expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be_zero
    end

    context "creation, edition" do 
      before :each do 
        @table_name =  "table#{Time.now.to_i}"
        @table_body = {  "tableReference"=> {
                            "projectId"=> @project,
                            "datasetId"=> @name,
                            "tableId"=> @table_name}, 
                "schema"=> [{:id=> "name", :type=> "string", :mode => "REQUIRED"},
                            {:id=>  "age", :type=> "integer"},
                            {:id=> "weight", :type=> "float"},
                            {:id=> "is_magic", :type=> "boolean"}]
              }
      end

      it ".create & .delete" do
        #If successful, this method returns a Tables resource in the response body.
        expect(GoogleBigquery::Table.create(@project, @name, @table_body )["tableReference"]["tableId"]).to eql @table_name
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 1
        GoogleBigquery::Table.delete(@project, @name, @table_name )
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 0
      end

      it ".create & .update .delete" do
        #If successful, this method returns a Tables resource in the response body.
        expect(GoogleBigquery::Table.create(@project, @name, @table_body )["tableReference"]["tableId"]).to eql @table_name
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 1
        
        expect(
          GoogleBigquery::Table.update(@project, @name, @table_name,
            {"tableReference"=> {
             "projectId" => @project,
             "datasetId" =>@name,
             "tableId"  => @table_name }, 
            "description"=> "foobar"} )["description"]
          ).to include "foobar"

        GoogleBigquery::Table.delete(@project, @name, @table_name )
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 0
      end


      it ".create & .update .delete" do
        #If successful, this method returns a Tables resource in the response body.
        expect(GoogleBigquery::Table.create(@project, @name, @table_body )["tableReference"]["tableId"]).to eql @table_name
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 1
        
        expect(
          GoogleBigquery::Table.patch(@project, @name, @table_name,
            {"tableReference"=> {
             "projectId" => @project,
             "datasetId" =>@name,
             "tableId"  => @table_name }, 
            "description"=> "foobar"} )["description"]
          ).to include "foobar"

        GoogleBigquery::Table.delete(@project, @name, @table_name )
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 0
      end

    end

  end

end
