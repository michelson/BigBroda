# GoogleBigquery

Big query client built on top of google api client.

https://developers.google.com/bigquery/what-is-bigquery


## Installation

Add this line to your application's Gemfile:

    gem 'google_bigquery'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install google_bigquery

# Usage:

## Rails / ActiveRecord:

### Active Record Adapter

#### Connection

ActiveRecord connection in plain ruby:

```ruby
    ActiveRecord::Base.establish_connection(
      :adapter => 'bigquery', 
      :project => "MyBigQueryProject",
      :database => "MyBigTable"
    )
```

In Rails app you can use the :adapter, :project and :database options in your database.yml or use the stablish connection in specific models.

#### Quering

  The GoogleBigQuery Adapter brings some of the ActiveRecord nicieties out of the box:

```ruby
User.all
User.first, User.last
User.count
User.find_by(name: "")
User.select("name")
User.select("name").where("name contains ?", "frank")
User.select("name, id").where("name contains ?", "frank").count
User.where("id =? and name= ?", "some-id-1393025921", "Frank")
User.where.not("admin = ?", false)
User.admins.joins(:posts)
```

#### Note about Joins:

BigQuery supports two types of JOIN operations:

  + JOIN requires that the right-side table contains less than 8 MB of compressed data.
  + JOIN EACH allows join queries for tables of any size.

BigQuery supports INNER and LEFT OUTER joins. The default is INNER.

see more at: https://developers.google.com/bigquery/query-reference#joins



#### Creation:

```ruby
  User.create(name: "frank capra")
  @user  = User.new
  @user.name = "Frank"
  @user.save
```

NOTE: by default the adapter will set Id values as an SecureRandom.hex, and for now all the foreign keys are created as a STRING type 

#### Deletion and edition of single rows:

  BigQuery tables are append-only. The query language does not currently support either updating or deleting data. In order to update or delete data, you must delete the table, then recreate the table with new data. Alternatively, you could write a query that modifies the data and specify a new results table.

  I would actually recommend creating a new table for each day. Since BigQuery charges by amount of data queried over, this would be most economical for you, rather than having to query over entire massive datasets every time.

  By the way - how are you currently collecting your data?



### Migrations:

This adapter has migration support for simple operations

```ruby
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

class AddPublishedToUser < ActiveRecord::Migration
  def change
    add_column :users, :published, :boolean, default: true
  end
end

```

Note: Big query does not provide a way to update columns nor delete, so update_column, or remove_column migration are cancelled with and exeption.

## Standalone Client:

### Configuration setup:

  https://developers.google.com/bigquery/docs/authorization

  Configure GoogleBigquery client:

```ruby
GoogleBigquery::Config.setup do |config|
  config.pass_phrase = "notasecret"
  config.key_file    = /location/to_your/key_file.p12
  config.client_id   = "XXXXX.apps.googleusercontent.com"
  config.scope       = "https://www.googleapis.com/auth/bigquery"
  config.email       = "XXXXXX@developer.gserviceaccount.com"
end
```

  And authorize client:

```ruby
@auth = GoogleBigquery::Auth.new
@auth.authorize
```
  Then you are ready to go!


### Projects

  https://developers.google.com/bigquery/docs/reference/v2/projects

```ruby
GoogleBigquery::Project.list["projects"]
```

### Jobs

  https://developers.google.com/bigquery/docs/reference/v2/jobs

#### Exporting data into multiple files

BigQuery can export up to 1 GB of data per file. If you plan to export more than 1 GB, you can use a wildcard character to instruct BigQuery to export to multiple files.

Note: it may take a while.

```ruby
  GoogleBigquery::Jobs.export(project_id, dataset_id, table_id, bucket_location)
```

#### Query

```ruby
GoogleBigquery::Jobs.query(@project, {"query"=> "SELECT * FROM [#{@dataset_id}.#{@table_name}] LIMIT 1000" })
```


### Datasets

  https://developers.google.com/bigquery/docs/reference/v2/datasets

#### List:

```ruby
GoogleBigquery::Dataset.list(@project_id)
```

#### Create/Insert:

```ruby  
GoogleBigquery::Dataset.create(@project, {"datasetReference"=> { "datasetId" => @dataset_id }} )
```

#### Delete:

```ruby  
GoogleBigquery::Dataset.delete(@project, @dataset_id }} )
```

#### Update/Patch:

  Updates information in an existing dataset. The update method replaces the entire dataset resource, whereas the patch method only replaces fields that are provided in the submitted dataset resource.

```ruby  
GoogleBigquery::Dataset.update(@project, @dataset_id,
      {"datasetReference"=> {
       "datasetId" =>@dataset_id }, 
      "description"=> "foobar"} )
```


  Updates information in an existing dataset. The update method replaces the entire dataset resource, whereas the patch method only replaces fields that are provided in the submitted dataset resource. This method supports patch semantics.

```ruby
GoogleBigquery::Dataset.patch(@project, @dataset_id,
        {"datasetReference"=> {
         "datasetId" =>@dataset_id }, 
        "description"=> "foobar"} )
```


### Tables

  https://developers.google.com/bigquery/docs/reference/v2/tables

#### Create:

```ruby
@table_body = {  "tableReference"=> {
                    "projectId"=> @project,
                    "datasetId"=> @dataset_id,
                    "tableId"=> @table_name}, 
        "schema"=> [fields: 
                      {:name=> "name", :type=> "string", :mode => "REQUIRED"},
                      {:name=>  "age", :type=> "integer"},
                      {:name=> "weight", :type=> "float"},
                      {:name=> "is_magic", :type=> "boolean"}
                  ]
      }

GoogleBigquery::Table.create(@project, @dataset_id, @table_body
```   

#### Update:

```ruby
    GoogleBigquery::Table.update(@project, @dataset_id, @table_name,
        {"tableReference"=> {
         "projectId" => @project, "datasetId" =>@dataset_id, "tableId"  => @table_name }, 
        "description"=> "foobar"} )
```
       
#### Delete:

```ruby
GoogleBigquery::Table.delete(@project, @dataset_id, @table_name )
```

#### List:

```ruby
    GoogleBigquery::Table.list(@project, @dataset_id )
```

### Table Data
  
  https://developers.google.com/bigquery/docs/reference/v2/tabledata

#### InsertAll

Streaming data into BigQuery is free for an introductory period until January 1st, 2014. After that it will be billed at a flat rate of 1 cent per 10,000 rows inserted. The traditional jobs().insert() method will continue to be free. When choosing which import method to use, check for the one that best matches your use case. Keep using the jobs().insert() endpoint for bulk and big data loading. Switch to the new tabledata().insertAll() endpoint if your use case calls for a constantly and instantly updating stream of data.

```ruby
@rows =   {"rows"=> [
                      {
                        "insertId"=> Time.now.to_i.to_s,
                        "json"=> {
                          "name"=> "User #{Time.now.to_s}"
                        }
                      }
                    ]}

GoogleBigquery::TableData.create(@project, @name, @table_name , @rows )
```


#### List

```ruby
GoogleBigquery::TableData.list(@project, @dataset_id, @table_name)
```


## RESOURCES:

  https://developers.google.com/bigquery/loading-data-into-bigquery

  https://developers.google.com/bigquery/streaming-data-into-bigquery

### Api Explorer:

  https://developers.google.com/apis-explorer/#s/bigquery/v2/

### Google Big query developer guide

  https://developers.google.com/bigquery/docs/developers_guide

# Caveats:

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

 
#TODO:

ActiveRecord:
  + Test HBTM HMT Associations
  + Generate AR schema migration records OR a YAML SCHEMA MIGRATION
  + AR migration copy tables to update it (copy to gs:// , delete table, import table from gs://)
  + AR migrate record type
  + Make id and foreign keys types and values configurable
