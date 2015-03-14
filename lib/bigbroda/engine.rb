# -*- encoding : utf-8 -*-

require "active_record/connection_adapters/bigquery_adapter.rb"

module BigBroda
  class Engine < ::Rails::Engine

    isolate_namespace BigBroda
    #config.generators do |g|
    #  g.test_framework  :rspec,
    #                    :fixture_replacement => :factory_girl ,
    #                    :dir => "spec/factories"
    #  g.integration_tool :rspec
    #end

    #initializer "require BigBroda" do
    #end

  end

end
