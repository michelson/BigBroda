

require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "Table", :vcr => { :allow_unused_http_interactions => true } do
  before(:all) do
    VCR.use_cassette("Table/authorize_config") do
      config_setup
      @auth = GoogleBigquery::Auth.new
      @auth.authorize
      @project = config_options["email"].match(/(\d*)/)[0]
    end
  end

  context "operations" do 

    before :each do 
      VCR.use_cassette("Table/each_create") do
        @name = "rspec_schema"
        GoogleBigquery::Dataset.create(@project, {"datasetReference"=> { "datasetId" => @name }} )["id"]  
      end
    end

    after(:each) do 
      VCR.use_cassette("Table/each_delete") do
        GoogleBigquery::Dataset.delete(@project, @name) 
      end
    end

    it "list", :vcr do
      expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be_zero
    end

    context "creation, edition" do 
      before :each do 
        @table_name =  "users"
        @table_body = { "tableReference"=> {
                          "projectId"=> @project,
                          "datasetId"=> @name,
                          "tableId"=> @table_name},
              "schema"=> [:fields=>[
                                            {:name=> "name", :type=> "string", :mode => "REQUIRED"},
                                            {:name=> "age", :type=> "integer"},
                                            {:name=> "weight", :type=> "float"},
                                            {:name=> "is_magic", :type=> "boolean"}
                                    ]
                        ]
            }          
      end

      it ".create & .delete", :vcr do
        GoogleBigquery::Table.create(@project, @name, @table_body )
        #If successful, this method returns a Tables resource in the response body.
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 1
        GoogleBigquery::Table.delete(@project, @name, @table_name )
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 0
      end

      it ".create & .update .delete", :vcr do
        GoogleBigquery::Table.create(@project, @name, @table_body )

        #If successful, this method returns a Tables resource in the response body.
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 1
        
        opts =  {"tableReference"=> {
                            "projectId" => @project,
                            "datasetId" =>@name,
                            "tableId"  => @table_name },
                          "description"=> "foobar"}
                

        expect(
          GoogleBigquery::Table.update(@project, @name, @table_name, opts )["description"]
          ).to include "foobar"

        GoogleBigquery::Table.delete(@project, @name, @table_name )
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 0
      end


      it ".create & .update .delete", :vcr do
        GoogleBigquery::Table.create(@project, @name, @table_body )
        #If successful, this method returns a Tables resource in the response body.
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 1
        
        opts =  {"tableReference"=> {
                          "projectId" => @project,
                          "datasetId" =>@name,
                          "tableId"  => @table_name }, 
                          "description"=> "foobar"}
                 
        
        expect(
          GoogleBigquery::Table.update(@project, @name, @table_name, opts)["description"]
          ).to include "foobar"

        GoogleBigquery::Table.delete(@project, @name, @table_name )
        expect(GoogleBigquery::Table.list(@project, @name )["totalItems"]).to be 0
      end

    end

  end

end
