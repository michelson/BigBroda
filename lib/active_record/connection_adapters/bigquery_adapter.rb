require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'
require 'active_record/connection_adapters/abstract/schema_statements'
require 'arel/visitors/bind_visitor'


module ActiveRecord
  module BigQueryRailsHelpers
    def self.rails40?
      ActiveRecord::VERSION::MAJOR == 4 && ActiveRecord::VERSION::MINOR == 0
    end

    def self.rails41?
      ActiveRecord::VERSION::MAJOR == 4 && ActiveRecord::VERSION::MINOR == 1
    end

    def self.rails42?
      ActiveRecord::VERSION::MAJOR == 4 && ActiveRecord::VERSION::MINOR == 2
    end
  end

  module Error
    class Standard < StandardError; end
    class NotImplementedFeature < Standard
      def message
        "This Adapter doesn't offer updating single rows, Google Big query is append only by design"
      end
    end
    class NotImplementedColumnOperation < Standard
      def message
        "Google big query doesn't allow this column operation"
      end
    end

    class PendingFeature < Standard
      def message
        "Sorry, this is a pending feature, it will be implemented soon."
      end
    end
  end

  module ConnectionHandling # :nodoc:
    # bigquery adapter reuses BigBroda::Auth.
    def bigquery_connection(config)

      # Require database.
      unless config[:database]
        raise ArgumentError, "No database file specified. Missing argument: database"
      end
      db = BigBroda::Auth.authorized? ? BigBroda::Auth.client : BigBroda::Auth.new.authorize
      #db #quizas deberia ser auth.api o auth.client

      #In case we are using a bigquery adapter as standard config in database.yml
      #All models are BigQuery enabled
      ActiveRecord::Base.send :include, ActiveRecord::BigQueryPersistence
      ActiveRecord::SchemaMigration.send :include, ActiveRecord::BigQuerySchemaMigration
      ActiveRecord::Migrator.send :include, ActiveRecord::BigQueryMigrator
      ActiveRecord::Relation.send :include, ActiveRecord::BigQueryRelation
      ActiveRecord::Base.send :include, ActiveRecord::BigQuerying

      #db.busy_timeout(ConnectionAdapters::SQLite3Adapter.type_cast_config_to_integer(config[:timeout])) if config[:timeout]
      ConnectionAdapters::BigqueryAdapter.new(db, logger, config)
    rescue  => e
      raise e
      #Errno::ENOENT => error
      #if error.message.include?("No such file or directory")
      #  raise ActiveRecord::NoDatabaseError.new(error.message)
      #else
      #  raise error
      #end
    end
  end

  module BQConnector
    extend ActiveSupport::Concern
    module ClassMethods
      def establish_bq_connection(path)
        self.send :include, ActiveRecord::BigQueryPersistence
        self.send :include, ActiveRecord::BigQueryRelation
        self.send :include, ActiveRecord::BigQuerying
        establish_connection path
      end
    end
  end

  ActiveRecord::Base.send :include, BQConnector


  # = Active Record Persistence
  module BigQueryPersistence
    extend ActiveSupport::Concern

    def delete
      raise Error::NotImplementedFeature
    end

    module ClassMethods

    end

    private
    # Creates a record with values matching those of the instance attributes
    # and returns its id.
    def create_record(attribute_names = @attributes.keys)
      record_timestamps_hardcoded
      attributes_values = self.changes.values.map(&:last)

      row_hash = Hash[ [ self.changes.keys, attributes_values ].transpose ]
      new_id =  SecureRandom.hex
      @rows =   {"rows"=> [{
                            "insertId"=> Time.now.to_i.to_s,
                            "json"=> row_hash.merge("id"=> new_id)
                          }]
                }
      conn_cfg = self.class.connection_config
      result = BigBroda::TableData.create(conn_cfg[:project],
        conn_cfg[:database],
        self.class.table_name ,
        @rows )

      #raise result["error"]["errors"].map{|o| "[#{o['domain']}]: #{o['reason']} #{o['message']}" }.join(", ") if result["error"].present?
      #here we output the IN MEMORY id , because of the BQ latency
      self.id = new_id #||= new_id if self.class.primary_key

      @new_record = false
      id
    end

    #Partially copied from activerecord::Timezones
    def record_timestamps_hardcoded
      if self.record_timestamps
        current_time = current_time_from_proper_timezone

        all_timestamp_attributes.each do |column|
          if respond_to?(column) && respond_to?("#{column}=") && self.send(column).nil?
            write_attribute(column.to_s, current_time)
          end
        end
      end
    end

    # DISABLED FEATURE, Google Big query is append only by design.
    def update_record(attribute_names = @attributes.keys)
      raise Error::NotImplementedFeature
    end
  end

  # = Active Record Quering
  module BigQuerying
    def find_by_sql(sql, binds = [])
      cfg = ActiveRecord::Base.connection_config
      result_set = connection.select_all(sanitize_sql(sql), "#{name} Load", binds)
      column_types = {}

      if result_set.respond_to? :column_types
        column_types = result_set.column_types
      else
        ActiveSupport::Deprecation.warn "the object returned from `select_all` must respond to `column_types`"
      end
      # When AR BigQuery queries uses joins , the fields appear as [database.table].field ,
      # so at least we clean the class columns to initialize the record propperly
      #"whoa1393194159_users_id".gsub(/#{@config[:database]}_#{self.table_name}_/, "")
      result_set.instance_variable_set("@columns", result_set.columns.map{|o| o.gsub(/#{cfg[:database]}_#{self.table_name}_/, "") } )

      result_set.map { |record| instantiate(record, column_types) }
    end
  end

  # = Active Record Relation
  module BigQueryRelation
    extend ActiveSupport::Concern
      module ClassMethods
        def delete(id_or_array)
          raise Error::NotImplementedFeature
        end

        def update(id, attributes)
          raise Error::NotImplementedFeature
        end

        def destroy_all(conditions = nil)
          raise Error::NotImplementedFeature
        end

        def destroy(id)
          raise Error::NotImplementedFeature
        end

        def delete_all(conditions = nil)
          raise Error::NotImplementedFeature
        end

        def update_all(updates)
          raise Error::NotImplementedFeature
        end
      end
  end

  module BigQuerySchemaMigration

    def self.included base
      attr_accessor :migration_file_pwd
      base.instance_eval do
        def schema_migration_hash
          file = schema_migration_file("r")
          json = JSON.parse(file.read)
        end

        def schema_migration_path
          Dir.pwd + "/db/schema_migrations.json"
        end

        def schema_migration_file(mode="w+")
          file_pwd = Dir.pwd + "/db/schema_migrations.json"
          File.open( file_pwd, mode )
        end

        def create_table(limit=nil)
          @migration_file_pwd = Dir.pwd + "/db/schema_migrations.json"
          unless File.exists?(@migration_file_pwd)
            puts "SCHEMA MIGRATION HERE"
            version_options = {null: false}
            version_options[:limit] = limit if limit

            #connection.create_table(table_name, id: false) do |t|
            #  t.column :version, :string, version_options
            #end
            file = schema_migration_file
            file.puts({ db:{ table_name.to_sym => [] } }.to_json )
            file.close
            #connection.add_index table_name, :version, unique: true, name: index_name
          end
        end

        #def self.drop_table
        #  binding.pry
        #  File.delete(schema_migration_path)
        #end

        def delete_version(options)
          #versions = ActiveRecord::SchemaMigration.where(:version => version.to_s)
          version = options[:version]
          new_data = SchemaMigration.schema_migration_hash["db"]["schema_migrations"].delete_if{|o| o["version"] == version.to_s}
          hsh = {:db=>{:schema_migrations => new_data } }
          f = schema_migration_file
          f.puts hsh.to_json
          f.close
        end

        def create!(args, *opts)
          current_data = schema_migration_hash
          unless schema_migration_hash["db"]["schema_migrations"].map{|o| o["version"]}.include?(args[:version].to_s)
            hsh = {:db=>{:schema_migrations => current_data["db"]["schema_migrations"] << args } }
            f = schema_migration_file
            f.puts hsh.to_json
            f.close
          end
          true
        end

        def all
          schema_migration_hash["db"]["schema_migrations"]
        end

        def where(args)
          all.select{|o| o[args.keys.first.to_s] == args.values.first}
        end
      end
    end
  end

  module BigQueryMigrator

    def self.included base
      #overload class methods
      base.instance_eval do
        def get_all_versions
          SchemaMigration.all.map { |x| x["version"].to_i }.sort
        end

        def current_version
          sm_table = schema_migrations_table_name
          migration_file_pwd = Dir.pwd + "/db/schema_migrations.json"

          if File.exists?(migration_file_pwd)
            get_all_versions.max || 0
          else
            0
          end
        end

        def needs_migration?
          current_version < last_version
        end

        def last_version
          get_all_versions.min.to_i
          #last_migration.version
        end

        def last_migration #:nodoc:
          migrations(migrations_paths).last || NullMigration.new
        end

      end
      #overload instance methods
      base.class_eval do
        def current_version
          migrated.max || 0
        end

        def current_migration
          migrations.detect { |m| m["version"] == current_version }
        end

        #def migrated
        #  @migrated_versions ||= Set.new(self.class.get_all_versions)
        #end

        private

          def record_version_state_after_migrating(version)

            if down?
              migrated.delete(version)
              ActiveRecord::SchemaMigration.delete_version(:version => version.to_s)
            else
              migrated << version
              ActiveRecord::SchemaMigration.create!(:version => version.to_s)
            end
          end
      end
    end

    #alias :current :current_migration
  end

  module LoadOperations
    extend ActiveSupport::Concern
    module ClassMethods
      def bigquery_export(bucket_location = nil)
        bucket_location = bucket_location.nil? ? "#{table_name}.json" : bucket_location
        cfg = connection_config
        BigBroda::Jobs.export(cfg[:project],
          cfg[:database],
          table_name,
          "#{cfg[:database]}/#{bucket_location}")
      end

      def bigquery_load(bucket_location = [])
        bucket_location = bucket_location.empty? ? ["#{cfg[:database]}/#{table_name}.json"] : bucket_location
        cfg = connection_config
        fields = columns.map{|o| {name: o.name, type: o.sql_type, mode: "nullable" } }
        BigBroda::Jobs.load(cfg[:project],
          cfg[:database],
          table_name,
          bucket_location,
          fields)
      end

      def bigquery_import()
      end
    end
  end

  ActiveRecord::Base.send :include, LoadOperations
end


if ActiveRecord::VERSION::MAJOR == 4
  case ActiveRecord::VERSION::MINOR
  when 0
    require File.join(File.dirname(__FILE__), 'rails_41.rb')
  when 1
    require File.join(File.dirname(__FILE__), 'rails_41.rb')
  when 2
    require File.join(File.dirname(__FILE__), 'rails_42.rb')
  end
else
  raise "BigBroda only works on Rails 4.X version"
end