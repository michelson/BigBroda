require File.expand_path(File.dirname(__FILE__) + '../../../spec_helper')

require "active_record"
require "active_record/connection_adapters/bigquery_adapter.rb"

describe "ActiveRecord Adapter" do

  before :all do 
    config_setup
    @name = "whoa#{Time.now.to_i}"
    @project = config_options["email"].match(/(\d*)/)[0]
    
    @auth = GoogleBigquery::Auth.new
    @auth.authorize

    @table_name =  "users"
    @table_body = {  "tableReference"=> {
                        "projectId"=> @project,
                        "datasetId"=> @name,
                        "tableId"=> @table_name}, 
                      "schema"=> [:fields=>[ 
                                    {:name=> "id", :type=> "string"},
                                    {:name=> "name", :type=> "string", :mode => "REQUIRED"},
                                    {:name=>  "age", :type=> "integer"},
                                    {:name=> "weight", :type=> "float"},
                                    {:name=> "is_magic", :type=> "boolean"}
                                  ]
                      ]
                  }

    ActiveRecord::Base.establish_connection(
      :adapter => 'bigquery', 
      :project => @project,
      :database => @name
    )
  end

  before :each do 
    expect(
      GoogleBigquery::Dataset.create(@project, 
        {"datasetReference"=> { "datasetId" => @name }} )["id"]
      ).to include @name

    @table = GoogleBigquery::Table.create(@project, @name, @table_body )
    
    @rows =   {"rows"=> [
                          {
                            "insertId"=> Time.now.to_i.to_s,
                            "json"=> {
                              "id" => "some-id-#{Time.now.to_i.to_s}",
                              "name"=> "User #{Time.now.to_s}"
                            }
                          }
                        ]}
                        
    GoogleBigquery::TableData.create(@project, @name, @table_name , @rows )

  end

  after :each do 
    GoogleBigquery::Table.delete(@project, @name, @table_name )
    GoogleBigquery::Dataset.delete(@project, @name) 
  end

  describe "adapter" do 
    before :each do
      class User < ActiveRecord::Base 
        validates :name, presence: true
      end
      #User.table_name = "[#{@name}.#{@table_name}]"
      #User.first
      #binding.pry
    end

    it "creation" do 
      binding.pry
    end

    it "simple quering" do
      sleep 50
      binding.pry
      #User.select("name, id").where("name contains ?", "frank").count
      #User.select("name, id").where("name contains ?", "frank")
      #User.select("name, id")
      #User.create(name: "frank capra")
      #User.find_by(id: "some-id-1393025921")
      #User.where("id =? and name= ?", "some-id-1393025921", "User 2014-02-21 20:38:41 -0300")
      expect(User.count).to be 1
      expect(User.first).to be_an_instance_of User
      expect(User.all.size).to be 1
    end
  end
end