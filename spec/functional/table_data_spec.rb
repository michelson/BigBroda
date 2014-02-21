

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
    GoogleBigquery::Dataset.create(@project, {"datasetReference"=> { "datasetId" => @name }} )["id"]  
        @table_name =  "table#{Time.now.to_i}"
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

  after(:each) do 
    GoogleBigquery::Table.delete(@project, @name, @table_name )
    GoogleBigquery::Dataset.delete(@project, @name) 
  end

  it "insertAll" do 
    GoogleBigquery::TableData.create(@project, @name, @table_name , @rows )
    sleep 30
    expect(GoogleBigquery::Jobs.query(@project, {"query"=> "SELECT * FROM [#{@name}.#{@table_name}] LIMIT 1000" })["rows"].empty?).to be false
    expect(GoogleBigquery::TableData.list(@project, @name, @table_name)["rows"].empty?).to be false
  end


end
