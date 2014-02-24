# encoding: UTF-8

module GoogleBigquery
  class InstallGenerator < ::Rails::Generators::Base

    source_root File.expand_path("../../../templates", __FILE__)

    desc "Creates a BigQuery initializer."
    #class_option :orm

    def copy_initializer
      say_status("installing", "BigQuery", :green)
      copy_file "bigquery.rb.erb", "config/initializers/bigquery.rb"
    end

    def show_readme
      readme "README" if behavior == :invoke
    end

  end
end
