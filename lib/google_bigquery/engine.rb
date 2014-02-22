# -*- encoding : utf-8 -*-

require "active_record/connection_adapters/bigquery_adapter.rb"

module GoogleBigquery
  class Engine < ::Rails::Engine
    
    isolate_namespace GoogleBigquery
    #config.generators do |g|
    #  g.test_framework  :rspec,
    #                    :fixture_replacement => :factory_girl ,
    #                    :dir => "spec/factories"
    #  g.integration_tool :rspec
    #end

    #initializer "require GoogleBigquery" do 
    #end

  end

end
