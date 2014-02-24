module ActiveRecord
  module Tasks # :nodoc:
    #extend ActiveSupport::Autoload
   
    class BigQueryDatabaseTasks 

      include ActiveRecord::Tasks::DatabaseTasks

      delegate :connection, :establish_connection, to: ActiveRecord::Base

      ActiveRecord::Tasks::DatabaseTasks.register_task(/bigquery/, ActiveRecord::Tasks::BigQueryDatabaseTasks)

      def create
        establish_connection configuration
        connection.create_database configuration['database']
        establish_connection configuration
      end

      def drop
        establish_connection configuration
        connection.drop_database configuration['database']
        establish_connection configuration
      end

      def initialize(configuration)
        @configuration = configuration
      end

      private

      def configuration
        @configuration
      end

      def configuration_without_database
        configuration.merge('database' => nil)
      end
    end

  end

end