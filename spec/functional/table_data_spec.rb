

require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')

describe "TableData", :vcr => { :allow_unused_http_interactions => true } do
  before(:all) do
    VCR.use_cassette("TableData/authorize_config") do
      config_setup
      @auth = GoogleBigquery::Auth.new
      @auth.authorize
      @project = config_options["email"].match(/(\d*)/)[0]
    end
  end

  before :each do 
    @name = "rspec_schema"
    VCR.use_cassette("TableData/create_each", :record => :new_episodes) do
      GoogleBigquery::Dataset.create(@project, {"datasetReference"=> { "datasetId" => @name }} )["id"]  
        @table_name =  "users"
        @table_body = {  "tableReference"=> {
                            "projectId"=> @project,
                            "datasetId"=> @name,
                            "tableId"=> @table_name}, 
                "schema"=> [:fields=>[ 
                                              {:name=> "name", :type=> "string", :mode => "REQUIRED"},
                                              {:name=>  "age", :type=> "integer"},
                                              {:name=> "weight", :type=> "float"},
                                              {:name=> "is_magic", :type=> "boolean"}
                                      ]
                          ]
              }
      @table = GoogleBigquery::Table.create(@project, @name, @table_body )
  
      @rows =   {"rows"=> [
                            {
                              "insertId"=> Time.now.to_i.to_s,
                              "json"=> {
                                "name"=> "User #{Time.now.to_s}"
                              }
                            }
                          ]}
    end
  end

  after(:each) do 
    VCR.use_cassette("TableData/delete_each") do
      GoogleBigquery::Dataset.delete(@project, @name) 
    end
  end

  it "insertAll" do 
    VCR.use_cassette("TableData/insertAll2", :record => :new_episodes) do
      GoogleBigquery::TableData.create(@project, @name, @table_name , @rows )
      #sleep 60
      expect(GoogleBigquery::Jobs.query(@project, {"query"=> "SELECT * FROM [#{@name}.#{@table_name}] LIMIT 1000" })["rows"].empty?).to be false
      expect(GoogleBigquery::TableData.list(@project, @name, @table_name)["rows"].empty?).to be false
    end
  end


end
