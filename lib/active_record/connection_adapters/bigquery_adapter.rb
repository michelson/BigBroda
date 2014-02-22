require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'
require 'arel/visitors/bind_visitor'

#gem 'sqlite3', '~> 1.3.6'
#require 'sqlite3'

module ActiveRecord


  module Error
    class Standard < StandardError; end

    class NotImplementedFeature < Standard
      def message
        "This Adapter doesn't offer updating single rows, Google Big query is append only by design"
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

      auth = GoogleBigquery::Auth.new
      auth.authorize
      db = auth
      #db = GoogleBigquery::Dataset.get(
      #  config[:project], 
      #  config[:database]
      #) 

      #SQLite3::Database.new(
      #  config[:database].to_s,
      #  :results_as_hash => true
      #)
      #db.busy_timeout(ConnectionAdapters::SQLite3Adapter.type_cast_config_to_integer(config[:timeout])) if config[:timeout]
      ConnectionAdapters::BigqueryAdapter.new(db, logger, config)
    rescue  => e
      #Errno::ENOENT => error
      #if error.message.include?("No such file or directory")
      #  raise ActiveRecord::NoDatabaseError.new(error.message)
      #else
      #  raise error
      #end
      binding.pry
    end
  end

  # = Active Record Persistence
  module Persistence
    extend ActiveSupport::Concern

    def delete
      raise Error::NotImplementedFeature
    end

    private
    # Creates a record with values matching those of the instance attributes
    # and returns its id.
    def create_record(attribute_names = @attributes.keys)
      
      attributes_values = arel_attributes_with_values_for_create(attribute_names)

      row_hash = Hash[ [attribute_names, attributes_values.map{|o| o.last}].transpose ]
      new_id =  SecureRandom.hex
      @rows =   {"rows"=> [
                          {
                            "insertId"=> Time.now.to_i.to_s,
                            "json"=> row_hash.merge("id"=> new_id)
                          }
                        ]}

      GoogleBigquery::TableData.create(ActiveRecord::Base.connection_config[:project], 
        ActiveRecord::Base.connection_config[:database], 
        self.class.table_name , 
        @rows )

      #binding.pry

      #new_id =  #self.class.unscoped.insert attributes_values
      self.id = new_id #||= new_id if self.class.primary_key

      @new_record = false
      id
    end

    # DISABLED FEATURE, Google Big query is append only by design.
    def update_record(attribute_names = @attributes.keys)
      raise Error::NotImplementedFeature
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

  module ConnectionAdapters
    
    class BigqueryColumn < Column
      class << self
        def binary_to_string(value)
          if value.encoding != Encoding::ASCII_8BIT
            value = value.force_encoding(Encoding::ASCII_8BIT)
          end
          value
        end
      end
    end
    
    class BigqueryAdapter < AbstractAdapter
      
      class Version
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
        false
      end

      # Returns true, since this connection adapter supports prepared statement
      # caching.
      def supports_statement_cache?
        false
      end

      # Returns true, since this connection adapter supports migrations.
      def supports_migrations? #:nodoc:
        false
      end

      def supports_primary_key? #:nodoc:
        false
      end

      def requires_reloading?
        false
      end

      def supports_add_column?
        false
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
        false
      end

      # Returns true
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

      def native_database_types #:nodoc:
        {
          #:primary_key => default_primary_key_type,
          :string      => { :name => "STRING" },
          #:text        => { :name => "text" },
          :integer     => { :name => "INTEGER" },
          :float       => { :name => "FLOAT" },
          #:decimal     => { :name => "decimal" },
          #:datetime    => { :name => "datetime" },
          #:timestamp   => { :name => "datetime" },
          :timestamp    => { name: "TIMESTAMP" },
          #:time        => { :name => "time" },
          #:date        => { :name => "date" },
          #:binary      => { :name => "blob" },
          :boolean     => { :name => "BOOLEAN" }
        }
      end

      # Returns the current database encoding format as a string, eg: 'UTF-8'
      def encoding
        @connection.encoding.to_s
      end

      # Returns true.
      def supports_explain?
        true
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

      def type_cast(value, column) # :nodoc:
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
        sql = "EXPLAIN QUERY PLAN #{to_sql(arel, binds)}"
        ExplainPrettyPrinter.new.pp(exec_query(sql, 'EXPLAIN', binds))
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
            # "SELECT * FROM [#{@name}.#{@table_name}] LIMIT 1000"
            #binding.pry
            result = GoogleBigquery::Jobs.query(@config[:project], {"query"=> sql })
            cols    = result["schema"]["fields"].map{|o| o["name"]} #stmt.columns
            records = result["rows"].map{|o| o["f"].map{|k,v| k["v"]} }
            stmt = records
          else
            binding.pry
            cache = @statements[sql] ||= {
              :stmt => @connection.prepare(sql)
            }
            stmt = cache[:stmt]
            cols = cache[:cols] ||= stmt.columns
            #stmt.reset!
            #stmt.bind_params binds.map { |col, val|
            #  type_cast(val, col)
            #}
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

      def update_sql(sql, name = nil) #:nodoc:
        super
        @connection.changes
      end

      def delete_sql(sql, name = nil) #:nodoc:
        sql += " WHERE 1=1" unless sql =~ /WHERE/i
        super sql, name
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

        schema["schema"]["fields"].map do |field|
          mode = field['mode'].present? && field['mode'] == "REQUIRED" ? false : true
          BigqueryColumn.new(field['name'], field['name'], field['type'], mode )
        end
      end

      # Returns an array of indexes for the given table.
      def indexes(table_name, name = nil) #:nodoc:
        binding.pry
      end

      def primary_key(table_name) #:nodoc:
        "id"
      end

      protected
        def select(sql, name = nil, binds = []) #:nodoc:
          exec_query(sql, name, binds)
        end

        def table_structure(table_name)
          #structure = exec_query("PRAGMA table_info(#{quote_table_name(table_name)})", 'SCHEMA').to_hash
          structure = GoogleBigquery::Table.get(@config[:project], @config[:database], table_name)["schema"]["fields"]
          raise(ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'") if structure.empty?
          structure
        end


    end
    
  end

end