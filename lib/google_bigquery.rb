$:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

require "google/api_client"
require "active_support"

module GoogleBigquery

  autoload  :VERSION,  'google_bigquery/version.rb'
  autoload  :Config,   'google_bigquery/config.rb'
  autoload  :Auth,     'google_bigquery/auth.rb'
  autoload  :Client,   'google_bigquery/client.rb'
  autoload  :Project,  'google_bigquery/project.rb'
  autoload  :Dataset,  'google_bigquery/dataset.rb'
  autoload  :Table,    'google_bigquery/table.rb'
  autoload  :TableData,'google_bigquery/table_data.rb'
  autoload  :Jobs,     'google_bigquery/jobs.rb'

  if defined?(::Rails::Railtie)
    autoload  :Rails,   'google_bigquery/engine.rb' if ::Rails.version >= '3.1'
  end

  if defined?(::Rails::Railtie)
    autoload  :Rails,   'google_bigquery/engine.rb' if ::Rails.version >= '3.1'
    require File.join(File.dirname(__FILE__), *%w[google_bigquery railtie]) if ::Rails.version.to_s >= '3.1'
  end

end
