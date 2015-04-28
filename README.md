# BigBroda

[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/michelson/BigBroda?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

GoogleBigQuery ActiveRecord Adapter & standalone API client

## Use Cases:

https://developers.google.com/bigquery/what-is-bigquery

BigQuery is fantastic for running ad hoc aggregate queries across a very very large dataset - large web logs, ad analysis, sensor data, sales data... etc. Basically, many kinds of "full table scan" queries. Queries are written in a SQL-style language (you don't have to write custom MapReduce functions).

But!, Bigquery has a constraint to consider before diving in,
BQ is append only , that means that you can't update records or delete them.

So, use BigQuery as an OLAP (Online Analytical Processing) service, not as OLTP (Online Transactional Processing). In other words, use BigQuery as a DataWareHouse.

## Installation

Add 'bigbroda' to your application's Gemfile or install it yourself as:

    $ gem install bigbroda

## Rails / ActiveRecord:

This gem supports ActiveRecord 4.0 / 4.1.

Support for 4.2 is on the way!.

#### Configure GoogleBigQuery:

    rails g bigbroda:install

Or generate a file in config/initializers/bigquery.rb with the following contents:

```ruby
BigBroda::Config.setup do |config|
  config.pass_phrase = ["pass_phrase"]
  config.key_file    = ["key_file"]
  config.scope       = ["scope"]
  config.email       = ["email"]
  config.retries     = [retries]
end
```

retries indicates the number of times to retry on recoverable errors (no retries if set to one or not present)

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

In Rails app you can use the :adapter, :project and :database options in your database.yml or use the ```establish_bq_connection(bq_connection)``` connection in specific models.

```yaml
development:
  adapter: sqlite3
  database: db/development.sqlite3
  pool: 5

bigquery:
  database: "dummy_dev"
  adapter: 'bigquery'
  project: 123456
  #database: "dummy_test"
```

By default if you set the development/production/test BD configuration as a bigquery connection all models are Bigquery, migrations and rake:db operations use the BigQuery migration system.

If you don't want to make all your models BigQuery you can set up specific BQ activeRecord models this way:

```ruby
class UserLog < ActiveRecord::Base
  establish_bq_connection "bigquery"
end
```

Then you will have to execute the migration programaticly. like this:

```ruby
UserMigration.up
```
or

```ruby
AddPublishedToUser.change
```

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

```ruby
User.create([{name: "miki"}, {name: "jara"}])

```

NOTE: by default the adapter will set Id values as an SecureRandom.hex, and for now all the foreign keys are created as a STRING type

#### Deletion and edition of single rows:

  BigQuery tables are append-only. The query language does not currently support either updating or deleting data. In order to update or delete data, you must delete the table, then recreate the table with new data. Alternatively, you could write a query that modifies the data and specify a new results table.

  I would actually recommend creating a new table for each day. Since BigQuery charges by amount of data queried over, this would be most economical for you, rather than having to query over entire massive datasets every time.

  By the way - how are you currently collecting your data?


### Massive Export / Import of data

Google Bigquery allows to import and export large datasets of data the default formats are JSON and CSV, currently the adapter is only able to export JSON format.

#### Export

The export can be acomplished very easy from an active record model as:
```ruby
User.bigquery_export(destination)
```
where destination should be a valid google cloud store uri. The adapter will manage that , so you only need to pass the file name. Example:

    User.bigquery_export("file.json")

the adapter will convert that option to gs://[configured_database]/[file.json]. Just be sure to create the bucket propperly in Cloud Storage panel.
Also if you don't pass the file argument you will get an generated uri like: gs://[configured_database]/[table_name].json.

#### Import

There are two ways to import massive data in bigquery, one is from a file from google cloud store and the second is from multipart Post

From google cloud storage:

```ruby
User.bigquery_import([an_array_with_paths_to_gs_uris])
```

From multipart/related post:

    PENDING

### Migrations:

This adapter has migration support migrations built in, but

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

Note:
+ Big query does not provide a way to update columns nor delete, so update_column, or remove_column migration are cancelled with an catched exception.
+ Also the schema_migrations table is not created in DB, is created as a json file in db/schema_migrations.json instead. Be sure to add the file in your version control.


## Standalone Client:

### Configuration setup:

  https://developers.google.com/bigquery/docs/authorization

  Configure BigBroda client:

```ruby
BigBroda::Config.setup do |config|
  config.pass_phrase = "notasecret"
  config.key_file    = /location/to_your/key_file.p12
  config.scope       = "https://www.googleapis.com/auth/bigquery"
  config.email       = "XXXXXX@developer.gserviceaccount.com"
  config.retries     = 1
end
```

retries indicates the number of times to retry on recoverable errors (no retries if set to one or not present)

  And authorize client:

```ruby
@auth = BigBroda::Auth.new
@auth.authorize
```
  Then you are ready to go!


### Projects

  https://developers.google.com/bigquery/docs/reference/v2/projects

```ruby
BigBroda::Project.list["projects"]
```

### Jobs

  https://developers.google.com/bigquery/docs/reference/v2/jobs

#### Exporting data into multiple files

BigQuery can export up to 1 GB of data per file. If you plan to export more than 1 GB, you can use a wildcard character to instruct BigQuery to export to multiple files.

Note: it may take a while.

```ruby
  BigBroda::Jobs.export(project_id, dataset_id, table_id, bucket_location)
```

#### Query

```ruby
BigBroda::Jobs.query(@project, {"query"=> "SELECT * FROM [#{@dataset_id}.#{@table_name}] LIMIT 1000" })
```


### Datasets

  https://developers.google.com/bigquery/docs/reference/v2/datasets

#### List:

```ruby
BigBroda::Dataset.list(@project_id)
```

#### Create/Insert:

```ruby
BigBroda::Dataset.create(@project, {"datasetReference"=> { "datasetId" => @dataset_id }} )
```

#### Delete:

```ruby
BigBroda::Dataset.delete(@project, @dataset_id }} )
```

#### Update/Patch:

  Updates information in an existing dataset. The update method replaces the entire dataset resource, whereas the patch method only replaces fields that are provided in the submitted dataset resource.

```ruby
BigBroda::Dataset.update(@project, @dataset_id,
      {"datasetReference"=> {
       "datasetId" =>@dataset_id },
      "description"=> "foobar"} )
```


  Updates information in an existing dataset. The update method replaces the entire dataset resource, whereas the patch method only replaces fields that are provided in the submitted dataset resource. This method supports patch semantics.

```ruby
BigBroda::Dataset.patch(@project, @dataset_id,
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

BigBroda::Table.create(@project, @dataset_id, @table_body
```

#### Update:

```ruby
    BigBroda::Table.update(@project, @dataset_id, @table_name,
        {"tableReference"=> {
         "projectId" => @project, "datasetId" =>@dataset_id, "tableId"  => @table_name },
        "description"=> "foobar"} )
```

#### Delete:

```ruby
BigBroda::Table.delete(@project, @dataset_id, @table_name )
```

#### List:

```ruby
    BigBroda::Table.list(@project, @dataset_id )
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

BigBroda::TableData.create(@project, @name, @table_name , @rows )
```


#### List

```ruby
BigBroda::TableData.list(@project, @dataset_id, @table_name)
```

## Testing:

### Install deps

`appraisal install`

### Run rspec suite for versions:

```
appraisal rails-3 rake spec
appraisal rails-4.0.3 rake spec
appraisal rails-4.1 rake spec
appraisal rails-4.2 rake spec
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
  + AR migration copy tables to update it (copy to gs:// , delete table, import table from gs://)
  + AR migrate BQ record type
  + Make id and foreign keys types and values configurable
  + Jobs make multipart/related upload

