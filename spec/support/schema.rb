require 'active_record'
require 'logger'

ActiveRecord::Base.establish_connection(
  :adapter => 'bigquery', 
  :database => ':memory:'
)
#ActiveRecord::Base.logger = Logger.new(SPEC_ROOT.join('debug.log'))
ActiveRecord::Migration.verbose = false

=begin
ActiveRecord::Schema.define do
  create_table :users do |t|
    t.string :name
    t.string :last_name
    t.string :email
    t.timestamps
  end
  create_table :general_models do |t|
    t.references :user, index: true
    t.string :name
    t.text :settings
    t.integer :position
    t.timestamps
  end

  create_table :importr_data_imports do |t|
    t.string :importer_type
    t.string :resource_type
    t.string :document
    t.boolean :finished, default: false
    t.references :user, index: true
    t.text :error_messages
    t.integer :success_count
    t.integer :error_count
    t.integer :processed_rows
    t.integer :total_rows
    t.string :uuid
    t.timestamps
  end
=end
end

