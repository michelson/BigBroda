require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'
require 'arel/visitors/bind_visitor'

module ActiveRecord

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
    # bigquery adapter reuses GoogleBigquery::Auth.
    def bigquery_connection(config)

      # Require database.
      unless config[:database]
        raise ArgumentError, "No database file specified. Missing argument: database"
      end
      db = GoogleBigquery::Auth.authorized? ? GoogleBigquery::Auth.client : GoogleBigquery::Auth.new.authorize
      #db #quizas deberia ser auth.api o auth.client
      
      #In case we are using a bigquery adapter as standard config in database.yml 
      #All models are BigQuery enabled
      ActiveRecord::Base.send :include, ActiveRecord::BigQueryPersistence

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

    private
    # Creates a record with values matching those of the instance attributes
    # and returns its id.
    def create_record(attribute_names = @attributes.keys)  
      #binding.pry
      record_timestamps_hardcoded
      attributes_values = self.changes.values.map(&:last)
      
      #binding.pry
      
      row_hash = Hash[ [ self.changes.keys, attributes_values ].transpose ]
      new_id =  SecureRandom.hex
      @rows =   {"rows"=> [{
                            "insertId"=> Time.now.to_i.to_s,
                            "json"=> row_hash.merge("id"=> new_id)
                          }]
                }
      conn_cfg = self.class.connection_config
      result = GoogleBigquery::TableData.create(conn_cfg[:project], 
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
  module Querying
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
      # so at least whe clean the class columns to initialize the record propperly
      #"whoa1393194159_users_id".gsub(/#{@config[:database]}_#{self.table_name}_/, "")
      result_set.instance_variable_set("@columns", result_set.columns.map{|o| o.gsub(/#{cfg[:database]}_#{self.table_name}_/, "") } )
      
      result_set.map { |record| instantiate(record, column_types) }
    end
  end

  # = Active Record Relation
  class Relation
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

  class SchemaMigration < ActiveRecord::Base
    #THIS SHOULD BE A YAML SCHEMA MIGRATION SYSTEM
    attr_accessor :migration_file_pwd

    def self.schema_migration_hash
      file = schema_migration_file("r")
      json = JSON.parse(file.read)
    end

    def self.schema_migration_path
      Dir.pwd + "/db/schema_migrations.json"
    end

    def self.schema_migration_file(mode="w+")
      file_pwd = Dir.pwd + "/db/schema_migrations.json"
      File.open( file_pwd, mode )
    end

    def self.create_table(limit=nil)
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

    def self.delete_version(options)
      #versions = ActiveRecord::SchemaMigration.where(:version => version.to_s)
      version = options[:version]
      new_data = SchemaMigration.schema_migration_hash["db"]["schema_migrations"].delete_if{|o| o["version"] == version.to_s}
      hsh = {:db=>{:schema_migrations => new_data } }
      f = schema_migration_file
      f.puts hsh.to_json
      f.close
    end

    def self.create!(args, *opts)
      current_data = schema_migration_hash
      unless schema_migration_hash["db"]["schema_migrations"].map{|o| o["version"]}.include?(args[:version].to_s)
        hsh = {:db=>{:schema_migrations => current_data["db"]["schema_migrations"] << args } }
        f = schema_migration_file
        f.puts hsh.to_json
        f.close
      end
      true
    end

    def self.all
      schema_migration_hash["db"]["schema_migrations"]
    end

    def self.where(args)
      all.select{|o| o[args.keys.first.to_s] == args.values.first}
    end
  end

  class Migrator
    class << self
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

    def current_version
      migrated.max || 0
    end

    def current_migration
      migrations.detect { |m| m["version"] == current_version }
    end

    def migrated
      @migrated_versions ||= Set.new(self.class.get_all_versions)
    end

    alias :current :current_migration

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

  module ConnectionAdapters


    class BigqueryColumn < Column
      class << self
        TRUE_VALUES = [true, 1, '1', 'true', 'TRUE'].to_set
        FALSE_VALUES = [false, 0, '0','false', 'FALSE'].to_set

        def binary_to_string(value)
          if value.encoding != Encoding::ASCII_8BIT
            value = value.force_encoding(Encoding::ASCII_8BIT)
          end
          value
        end

        def string_to_time(string)
          #binding.pry
          #puts string
          return string unless string.is_a?(String)
          return nil if string.empty?
          fast_string_to_time(string) || fallback_string_to_time(string) || Time.at(string.to_f).send(Base.default_timezone)
        end
      end
    end
    
    class BigqueryAdapter < AbstractAdapter
      

      class Version
      end

      class ColumnDefinition < ActiveRecord::ConnectionAdapters::ColumnDefinition
        attr_accessor :array
      end

      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition

        def primary_key(name, type = :primary_key, options = {})
          return column name, :string, options
        end

        def record(*args)
          options = args.extract_options!
          column(:created_at, :record, options)
        end

        def timestamps(*args)
          options = args.extract_options!
          column(:created_at, :timestamp, options)
          column(:updated_at, :timestamp, options)
        end

        def references(*args)
          options = args.extract_options!
          polymorphic = options.delete(:polymorphic)
          index_options = options.delete(:index)
          args.each do |col|
            column("#{col}_id", :string, options)
            column("#{col}_type", :string, polymorphic.is_a?(Hash) ? polymorphic : options) if polymorphic
            index(polymorphic ? %w(id type).map { |t| "#{col}_#{t}" } : "#{col}_id", index_options.is_a?(Hash) ? index_options : {}) if index_options
          end
        end

      end

      class StatementPool < ConnectionAdapters::StatementPool
        def initialize(connection, max)
          super
          @cache = Hash.new { |h,pid| h[pid] = {} }
        end

        def each(&block); cache.each(&block); end
        def key?(key); cache.key?(key); end
        def [](key); cache[key]; end
        def length; cache.length; end

        def []=(sql, key)
          while @max <= cache.size
            dealloc(cache.shift.last[:stmt])
          end
          cache[sql] = key
        end

        def clear
          cache.values.each do |hash|
            dealloc hash[:stmt]
          end
          cache.clear
        end

        private
        def cache
          @cache[$$]
        end

        def dealloc(stmt)
          stmt.close unless stmt.closed?
        end
      end

      class BindSubstitution < Arel::Visitors::SQLite # :nodoc:
        include Arel::Visitors::BindVisitor
      end

      def initialize(connection, logger, config)
        super(connection, logger)

        @active = nil
        @statements = StatementPool.new(@connection,
                                        self.class.type_cast_config_to_integer(config.fetch(:statement_limit) { 1000 }))
        @config = config

        if self.class.type_cast_config_to_boolean(config.fetch(:prepared_statements) { true })
          @prepared_statements = true
          @visitor = Arel::Visitors::SQLite.new self
        else
          @visitor = unprepared_visitor
        end
      end

      def adapter_name #:nodoc:
        'BigQuery'
      end

      def supports_ddl_transactions?
        false
      end

      def supports_savepoints?
        false
      end

      def supports_partial_index?
        true
      end

      # Returns true, since this connection adapter supports prepared statement
      # caching.
      def supports_statement_cache?
        true
      end

      # Returns true, since this connection adapter supports migrations.
      def supports_migrations? #:nodoc:
        true
      end

      def supports_primary_key? #:nodoc:
        true
      end

      def requires_reloading?
        false
      end

      def supports_add_column?
        true
      end

      def active?
        @active != false
      end

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        super
        @active = false
        @connection.close rescue nil
      end

      # Clears the prepared statements cache.
      def clear_cache!
        @statements.clear
      end

      def supports_index_sort_order?
        true
      end

      # Returns true
      def supports_count_distinct? #:nodoc:
        true
      end

      # Returns false
      def supports_autoincrement? #:nodoc:
        false
      end

      def supports_index_sort_order?
        false
      end

      # Returns 62. SQLite supports index names up to 64
      # characters. The rest is used by rails internally to perform
      # temporary rename operations
      def allowed_index_name_length
        index_name_length - 2
      end

      def default_primary_key_type
        if supports_autoincrement?
          'STRING'
        else
          'STRING'
        end
      end

      def native_database_types #:nodoc:
        {
          :primary_key => default_primary_key_type,
          :string      => { :name => "STRING", :default=> nil },
          #:text        => { :name => "text" },
          :integer     => { :name => "INTEGER", :default=> nil },
          :float       => { :name => "FLOAT", :default=> 0.0 },
          #:decimal     => { :name => "decimal" },
          :datetime    => { :name => "TIMESTAMP" },
          #:timestamp   => { :name => "datetime" },
          :timestamp    => { name: "TIMESTAMP" },
          #:time        => { :name => "time" },
          #:date        => { :name => "date" },
          :record      => { :name => "RECORD" },
          :boolean     => { :name => "BOOLEAN" }
        }
      end

      # Returns the current database encoding format as a string, eg: 'UTF-8'
      def encoding
        @connection.encoding.to_s
      end

      # Returns false.
      def supports_explain?
        false
      end

      def create_database(database)
        result = GoogleBigquery::Dataset.create(@config[:project], 
          {"datasetReference"=> { "datasetId" => database }} )
        #raise result["error"]["errors"].map{|o| "[#{o['domain']}]: #{o['reason']} #{o['message']}" }.join(", ") if result["error"].present?
        result
      end

      def drop_database(database)
        tables = GoogleBigquery::Table.list(@config[:project], database)["tables"]
        unless tables.blank?
          tables.map!{|o| o["tableReference"]["tableId"]} 
          tables.each do |table_id|
            GoogleBigquery::Table.delete(@config[:project], database, table_id)
          end
        end
        result = GoogleBigquery::Dataset.delete(@config[:project], database )
         if result == true
            File.delete(SchemaMigration.schema_migration_path) rescue ""
            return true
          end
        #raise result["error"]["errors"].map{|o| "[#{o['domain']}]: #{o['reason']} #{o['message']}" }.join(", ") if result["error"].present?
        result
      end

      # QUOTING ==================================================

      def quote(value, column = nil)
        if value.kind_of?(String) && column && column.type == :binary && column.class.respond_to?(:string_to_binary)
          s = column.class.string_to_binary(value).unpack("H*")[0]
          "x'#{s}'"
        else
          super
        end
      end

      def quote_table_name(name)
        "#{@config[:database]}.#{name}"
      end

      def quote_table_name_for_assignment(table, attr)
        quote_column_name(attr)
      end

      def quote_column_name(name) #:nodoc:
        name
      end

      # Quote date/time values for use in SQL input. Includes microseconds
      # if the value is a Time responding to usec.
      def quoted_date(value) #:nodoc:
        if value.respond_to?(:usec)
          "#{super}.#{sprintf("%06d", value.usec)}"
        else
          super
        end
      end

      def quoted_true
        "1"
      end

      def quoted_false
        "0"
      end

      def type_cast(value, column) # :nodoc:
        #binding.pry
        return value.to_f if BigDecimal === value
        return super unless String === value
        return super unless column && value

        value = super
        if column.type == :string && value.encoding == Encoding::ASCII_8BIT
          logger.error "Binary data inserted for `string` type on column `#{column.name}`" if logger
          value = value.encode Encoding::UTF_8
        end
        value
      end

      # DATABASE STATEMENTS ======================================

      def explain(arel, binds = [])
        #sql = "EXPLAIN QUERY PLAN #{to_sql(arel, binds)}"
        #ExplainPrettyPrinter.new.pp(exec_query(sql, 'EXPLAIN', binds))
      end

      class ExplainPrettyPrinter
        # Pretty prints the result of a EXPLAIN QUERY PLAN in a way that resembles
        # the output of the SQLite shell:
        #
        #   0|0|0|SEARCH TABLE users USING INTEGER PRIMARY KEY (rowid=?) (~1 rows)
        #   0|1|1|SCAN TABLE posts (~100000 rows)
        #
        def pp(result) # :nodoc:
          result.rows.map do |row|
            row.join('|')
          end.join("\n") + "\n"
        end
      end

      def exec_query(sql, name = nil, binds = [])
        log(sql, name, binds) do
          
          # Don't cache statements if they are not prepared
          if without_prepared_statement?(binds)
            result = GoogleBigquery::Jobs.query(@config[:project], {"query"=> sql })
            #raise result["error"]["errors"].map{|o| "[#{o['domain']}]: #{o['reason']} #{o['message']}" }.join(", ") if result["error"].present?
            cols    = result["schema"]["fields"].map{|o| o["name"] }
            records = result["totalRows"].to_i.zero? ? [] : result["rows"].map{|o| o["f"].map{|k,v| k["v"]} }
            stmt = records
          else
            cache = @statements[sql] ||= {
              :stmt => @connection.prepare(sql)
            }
            stmt = cache[:stmt]
            cols = cache[:cols] ||= stmt.columns
            #stmt.reset!
            stmt.bind_params binds.map { |col, val|
              type_cast(val, col)
            }
          end

          ActiveRecord::Result.new(cols, stmt)
        end
      end

      def exec_delete(sql, name = 'SQL', binds = [])
        exec_query(sql, name, binds)
        @connection.changes
      end

      alias :exec_update :exec_delete

      def last_inserted_id(result)
        @connection.last_insert_row_id
      end

      def execute(sql, name = nil) #:nodoc:
        log(sql, name) { @connection.execute(sql) }
      end

      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
        super
        id_value || @connection.last_insert_row_id
      end
      alias :create :insert_sql

      def select_rows(sql, name = nil)
        exec_query(sql, name).rows
      end

      def begin_db_transaction #:nodoc:
        log('begin transaction',nil) {  } #@connection.transaction
      end

      def commit_db_transaction #:nodoc:
        log('commit transaction',nil) {  } #@connection.commit
      end

      def rollback_db_transaction #:nodoc:
        log('rollback transaction',nil) { } #@connection.rollback
      end

      # SCHEMA STATEMENTS ========================================

      def tables(name = nil, table_name = nil) #:nodoc:
        table = GoogleBigquery::Table.list(@config[:project], @config[:database])
        return [] if table["tables"].blank?
        table_names = table["tables"].map{|o| o["tableReference"]["tableId"]}
        table_names = table_names.select{|o| o == table_name } if table_name
        table_names
      end

      def table_exists?(table_name)
        table_name && tables(nil, table_name).any?
      end

      # Returns an array of +SQLite3Column+ objects for the table specified by +table_name+.
      def columns(table_name) #:nodoc:
        schema = GoogleBigquery::Table.get(@config[:project], @config[:database], table_name)
        #binding.pry
        schema["schema"]["fields"].map do |field|
          mode = field['mode'].present? && field['mode'] == "REQUIRED" ? false : true
          #column expects (name, default, sql_type = nil, null = true)
          BigqueryColumn.new(field['name'], nil, field['type'], mode )
        end
      end

      # Returns an array of indexes for the given table.
      def indexes(table_name, name = nil) #:nodoc:
        []
      end

      def primary_key(table_name) #:nodoc:
        "id"
      end

      def remove_index!(table_name, index_name) #:nodoc:
        #exec_query "DROP INDEX #{quote_column_name(index_name)}"
      end

      def add_column(table_name, column_name, type, options = {}) #:nodoc:
        if supports_add_column? && valid_alter_table_options( type, options )
          super(table_name, column_name, type, options)
        else
          alter_table(table_name) do |definition|
            definition.column(column_name, type, options)
          end
        end
      end

      # See also TableDefinition#column for details on how to create columns.
      def create_table(table_name, options = {})
        td = create_table_definition table_name, options[:temporary], options[:options]

        unless options[:id] == false
          pk = options.fetch(:primary_key) {
            Base.get_primary_key table_name.to_s.singularize
          }

          td.primary_key pk, options.fetch(:id, :primary_key), options
        end

        yield td if block_given?

        if options[:force] && table_exists?(table_name)
          drop_table(table_name, options)
        end
        

        hsh = td.columns.map { |c|  {"name"=> c[:name], "type"=> c[:type] }  }

        @table_body = {  "tableReference"=> {
                            "projectId"=> @config[:project],
                            "datasetId"=> @config[:database],
                            "tableId"=> td.name}, 
                          "schema"=> [fields: hsh]
                      }

        res = GoogleBigquery::Table.create(@config[:project], @config[:database], @table_body )

        raise res["error"]["errors"].map{|o| "[#{o['domain']}]: #{o['reason']} #{o['message']}" }.join(", ") if res["error"].present?
      end

      # See also Table for details on all of the various column transformation.
      def change_table(table_name, options = {})
        if supports_bulk_alter? && options[:bulk]
          recorder = ActiveRecord::Migration::CommandRecorder.new(self)
          yield update_table_definition(table_name, recorder)
          bulk_change_table(table_name, recorder.commands)
        else
          yield update_table_definition(table_name, self)
        end
      end
      # Renames a table.
      #
      # Example:
      #   rename_table('octopuses', 'octopi')
      def rename_table(table_name, new_name)
        raise Error::PendingFeature
      end

      # See: http://www.sqlite.org/lang_altertable.html
      # SQLite has an additional restriction on the ALTER TABLE statement
      def valid_alter_table_options( type, options)
        type.to_sym != :primary_key
      end

      def add_column(table_name, column_name, type, options = {}) #:nodoc:
       
        if supports_add_column? && valid_alter_table_options( type, options )
        
          hsh = table_name.classify.constantize.columns.map { |c|  {"name"=> c.name, "type"=> c.type }  }
          hsh << {"name"=> column_name, :type=> type}
          fields = [ fields: hsh ]

          res = GoogleBigquery::Table.patch(@config[:project], @config[:database], table_name,
            {"tableReference"=> {
             "projectId" => @config[:project],
             "datasetId" =>@config[:database],
             "tableId"  => table_name }, 
             "schema"   => fields,
            "description"=> "added from migration"} )
        
        else
          bypass_feature
          #alter_table(table_name) do |definition|
          #  definition.column(column_name, type, options)
          #end
        end
      end

      def bypass_feature
        begin
          raise Error::NotImplementedColumnOperation
        rescue => e
          puts e.message
          logger.warn(e.message)
        end
      end

      def remove_column(table_name, column_name, type = nil, options = {}) #:nodoc: 
        bypass_feature
      end

      def change_column_default(table_name, column_name, default) #:nodoc:
        bypass_feature
      end

      def change_column_null(table_name, column_name, null, default = nil)
        bypass_feature
      end

      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        bypass_feature
      end

      def rename_column(table_name, column_name, new_column_name) #:nodoc:
        bypass_feature
      end

      def add_reference(table_name, ref_name, options = {})
        polymorphic = options.delete(:polymorphic)
        index_options = options.delete(:index)
        add_column(table_name, "#{ref_name}_id", :string, options)
        add_column(table_name, "#{ref_name}_type", :string, polymorphic.is_a?(Hash) ? polymorphic : options) if polymorphic
        add_index(table_name, polymorphic ? %w[id type].map{ |t| "#{ref_name}_#{t}" } : "#{ref_name}_id", index_options.is_a?(Hash) ? index_options : nil) if index_options
      end

      def drop_table(table_name)
        GoogleBigquery::Table.delete(@config[:project], @config[:database], table_name )
      end

      def dump_schema_information #:nodoc:
        sm_table = ActiveRecord::Migrator.schema_migrations_table_name
        rows = {"rows"=> [{"insertId"=> Time.now.to_i.to_s,
                              "json"=> {
                                "version"=> "#{sm.version}"
                              }}
                          ]}
        
        GoogleBigquery::TableData.create(@config[:project], @config[:database], sm_table , @rows )
      end

      def assume_migrated_upto_version(version, migrations_paths = ActiveRecord::Migrator.migrations_paths)
        binding.pry
        migrations_paths = Array(migrations_paths)
        version = version.to_i
        sm_table = quote_table_name(ActiveRecord::Migrator.schema_migrations_table_name)

        migrated = select_values("SELECT version FROM #{sm_table}").map { |v| v.to_i }
        paths = migrations_paths.map {|p| "#{p}/[0-9]*_*.rb" }
        versions = Dir[*paths].map do |filename|
          filename.split('/').last.split('_').first.to_i
        end

        unless migrated.include?(version)
          execute "INSERT INTO #{sm_table} (version) VALUES ('#{version}')"
        end

        inserted = Set.new
        (versions - migrated).each do |v|
          if inserted.include?(v)
            raise "Duplicate migration #{v}. Please renumber your migrations to resolve the conflict."
          elsif v < version
            execute "INSERT INTO #{sm_table} (version) VALUES ('#{v}')"
            inserted << v
          end
        end
      end


      protected
        def select(sql, name = nil, binds = []) #:nodoc:
          exec_query(sql, name, binds)
        end

        def table_structure(table_name)
          structure = GoogleBigquery::Table.get(@config[:project], @config[:database], table_name)["schema"]["fields"]
          raise(ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'") if structure.empty?
          structure
        end

        def alter_table(table_name, options = {}) #:nodoc:

        end

        def move_table(from, to, options = {}, &block) #:nodoc:
          copy_table(from, to, options, &block)
          drop_table(from)
        end

        def copy_table(from, to, options = {}) #:nodoc:
           
        end

        def copy_table_indexes(from, to, rename = {}) #:nodoc:
          
        end

        def copy_table_contents(from, to, columns, rename = {}) #:nodoc:

        end

        def create_table_definition(name, temporary, options)
          TableDefinition.new native_database_types, name, temporary, options
        end

    end


    
  end

end
