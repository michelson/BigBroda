

module ActiveRecord


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
          return string unless string.is_a?(String)
          return nil if string.empty?
          fast_string_to_time(string) || fallback_string_to_time(string) || Time.at(string.to_f).send(Base.default_timezone)
        end
      end
    end

    class BQBinary < Type::Binary # :nodoc:
      def cast_value(value)
        if value.encoding != Encoding::ASCII_8BIT
          value = value.force_encoding(Encoding::ASCII_8BIT)
        end
        value
      end
    end

    class BQString < Type::String # :nodoc:
      def type_cast_for_database(value)
        binding.pry
        if value.is_a?(::String) && value.encoding == Encoding::ASCII_8BIT
          value.encode(Encoding::UTF_8)
        else
          super
        end
      end

      def type_cast_from_user(value)
        binding.pry
      end
    end

    class BigqueryAdapter < AbstractAdapter

      #include SchemaStatements

      NATIVE_DATABASE_TYPES = {
        :primary_key => "STRING",
        :string      => { :name => "STRING", :default=> nil },
        :integer     => { :name => "INTEGER", :default=> nil },
        :float       => { :name => "FLOAT", :default=> 0.0 },
        :datetime    => { :name => "TIMESTAMP" },
        :timestamp    => { name: "TIMESTAMP" },
        :date        => { :name => "TIMESTAMP" },
        :record      => { :name => "RECORD" },
        :boolean     => { :name => "BOOLEAN" }
      }

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
        @config.merge(prepared_statements: false) if BigQueryRailsHelpers.rails40?
        @prepared_statements = false
        #if self.class.type_cast_config_to_boolean(config.fetch(:prepared_statements) { true })
        #  @prepared_statements = true
        #  @visitor = Arel::Visitors::SQLite.new self
        #else
        #use the sql without prepraded statements, as I know BQ doesn't support them.
        @type_map = Type::HashLookupTypeMap.new
        initialize_type_map(type_map)
        @visitor = unprepared_visitor unless ActiveRecord::VERSION::MINOR >= 2
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
        false
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
        NATIVE_DATABASE_TYPES
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
        result
      end

      # QUOTING ==================================================

      def _quote(value, column = nil)
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
        binding.pry
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
        bypass_feature
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
        binding.pry
        log(sql, name, binds) do

          # Don't cache statements if they are not prepared
          #if without_prepared_statement?(binds)
            result = GoogleBigquery::Jobs.query(@config[:project], {"query"=> sql })
            cols    = result["schema"]["fields"].map{|o| o["name"] }
            records = result["totalRows"].to_i.zero? ? [] : result["rows"].map{|o| o["f"].map{|k,v| k["v"]} }
            stmt = records
          #else
            #binding.pry
            #BQ does not support prepared statements, yiak!
          #end

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
        binding.pry
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

        hsh = td.columns.map { |c|  {"name"=> c[:name], "type"=> type_to_sql(c[:type]) }  }

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
      def valid_alter_table_type?(type)
        type.to_sym != :primary_key
      end

      def add_column(table_name, column_name, type, options = {}) #:nodoc:
        if valid_alter_table_type?(type)

          hsh = table_name.classify.constantize.columns.map { |c|  {"name"=> c.name, "type"=> c.cast_type }  }
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
        bypass_feature
      end

      def assume_migrated_upto_version(version, migrations_paths = ActiveRecord::Migrator.migrations_paths)
        bypass_feature
      end


      protected

        def initialize_type_map(m)
          super
          puts "INITILIZLIE TYPES MAP"
          m.register_type(/binary/i, BQBinary.new)
          register_class_with_limit m, %r(char)i, BQString
        end

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
