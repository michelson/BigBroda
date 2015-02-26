require File.expand_path(File.dirname(__FILE__) + '../../../spec_helper')

require "active_record"
require "active_record/connection_adapters/bigquery_adapter.rb"

class CreateUsers < ActiveRecord::Migration
  def self.up
    create_table :users do |t|
      t.string :name
      t.record :nested_data
      t.references :taggable, :polymorphic => true
      t.boolean :admin
      t.timestamps
    end
  end

  def self.down
    drop_table :users
  end
end

class CreatePosts < ActiveRecord::Migration
  def self.up
    create_table :posts do |t|
      t.string :title
      t.references :user
      t.boolean :admin
      t.timestamps
    end
  end

  def self.down
    drop_table :users
  end
end

class AddPublishedToUser < ActiveRecord::Migration
  def change
    add_column :users, :published, :boolean, default: true
  end
end

class RemovePublishedToUser < ActiveRecord::Migration
  def change
    remove_column :users, :published
  end
end

class User < ActiveRecord::Base
  validates :name, presence: true
  has_many :posts

  scope :admins , ->{where(admin: true)}
end

class Post < ActiveRecord::Base
  validates :title, presence: true
  belongs_to :user
end

def create_tables
  @table = GoogleBigquery::Table.create(@project, @name, @table_body )

  @rows =   {"rows"=> [
                        {
                          "insertId"=> Time.now.to_i.to_s,
                          "json"=> {
                            "name"=> "User #{Time.now.to_s}"
                          }
                        }
                      ]}

  GoogleBigquery::TableData.create(@project, @name, @table_name , @rows )
end

describe "ActiveRecord Adapter", :vcr => { :allow_unused_http_interactions => true } do

  let(:migration) { CreateUsers.new}
  let(:posts_migration) { CreatePosts.new}
  let(:add_col_migration) { AddPublishedToUser.new}
  let(:remove_col_migration) { RemovePublishedToUser.new}

  before :all do

    VCR.use_cassette("ActiveRecord_Adapter/authorize_config") do
      config_setup
      @auth = GoogleBigquery::Auth.new
      @auth.authorize
      @name = "rspec_schema"
      @project = config_options["email"].match(/(\d*)/)[0]

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
  end

  before :each do
    VCR.use_cassette("ActiveRecord_Adapter/create_each") do
      GoogleBigquery::Dataset.create(@project,
        {"datasetReference"=> { "datasetId" => @name }} )
      create_tables
    end
  end

  after :each do
    VCR.use_cassette("ActiveRecord_Adapter/after_each") do
      GoogleBigquery::Dataset.delete(@project, @name)
    end
  end

  describe "adapter" do

    it "simple quering", :vcr do
      #sleep 50
      #binding.pry
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

  describe "migrations" do

    before :each do
      VCR.use_cassette("ActiveRecord_Adapter/after_each") do
        GoogleBigquery::Table.delete(@project, @name, "users")
        migration.up; User.reset_column_information
      end
    end

    describe '#up', vcr: {:record => :new_episodes} do
      it 'adds the created_at & updated_at column', :vcr do
        User.columns_hash.should have_key('created_at')
        User.columns_hash.should have_key('updated_at')
      end
    end

    describe '#down', vcr: {:record => :new_episodes} do
      before {
        migration.down; User.reset_column_information
      }

      it 'adds the email_at_utc_hour column' do
        User.should_not be_table_exists
      end

    end

    #describe "add column", vcr: {:record => :new_episodes} do
    #  before {
    #    add_col_migration.change; User.reset_column_information
    #  }

    #  it 'adds published column' do
    #    #binding.pry
    #    User.columns_hash.should have_key('published')
    #  end
    #end

    describe "remove column", vcr: {:record => :new_episodes} do
      before {
        add_col_migration.change; User.reset_column_information
      }

      it 'should raise error' do
        expect{remove_col_migration.change}.to raise_error
      end
    end

    describe "associations", vcr: {:record => :new_episodes} do
      before {
        posts_migration.up; Post.reset_column_information
      }

      it "users_posts" do
        User.create(name: "ALF")
        #sleep 50
        post = User.first.posts.create(title: "yeah")
        #sleep 50
        expect(User.first).to respond_to(:id)
        expect(User.first.posts.first).to be_an_instance_of Post
        expect(User.joins(:posts).first.posts.count).to be 1
      end

    end

  end

end