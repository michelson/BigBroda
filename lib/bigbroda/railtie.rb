# encoding: UTF-8

module BigBroda
  class Railtie < ::Rails::Railtie
    config.before_configuration do
      require "#{Rails.root}/config/initializers/bigquery"
      BigBroda::Auth.new.authorize
    #  if config.action_view.javascript_expansions
    #    config.action_view.javascript_expansions[:high_charts] |= %w(highcharts exporting)
    #  end
    end

    config.after_initialize do
      #Google::APIClient.logger = Rails.logger
      Google::APIClient.logger = Logger.new("#{Rails.root}/log/bigquery-cli.log")
      #Logger.new(STDOUT)
    end

    rake_tasks do
      require "active_record/base"
      require "active_record/tasks/bigquery_database_tasks"

      #ActiveRecord::Tasks::DatabaseTasks.seed_loader = Rails.application
      #ActiveRecord::Tasks::DatabaseTasks.env = Rails.env

      #namespace :db do

      #end

      #load "active_record/railties/databases.rake"
    end




  end

end

