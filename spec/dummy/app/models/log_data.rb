class LogData < ActiveRecord::Base
  establish_connection "bigquery"
end
