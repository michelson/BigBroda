$:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

require "google/api_client"
require "active_support"

module BigBroda

  autoload  :VERSION,  'bigbroda/version.rb'
  autoload  :Config,   'bigbroda/config.rb'
  autoload  :Auth,     'bigbroda/auth.rb'
  autoload  :Client,   'bigbroda/client.rb'
  autoload  :Project,  'bigbroda/project.rb'
  autoload  :Dataset,  'bigbroda/dataset.rb'
  autoload  :Table,    'bigbroda/table.rb'
  autoload  :TableData,'bigbroda/table_data.rb'
  autoload  :Jobs,     'bigbroda/jobs.rb'

  if defined?(::Rails::Railtie)
    autoload  :Rails,   'bigbroda/engine.rb' if ::Rails.version >= '3.1'
  end

  if defined?(::Rails::Railtie)
    autoload  :Rails,   'bigbroda/engine.rb' if ::Rails.version >= '3.1'
    require File.join(File.dirname(__FILE__), *%w[bigbroda railtie]) if ::Rails.version.to_s >= '3.1'
  end

end
