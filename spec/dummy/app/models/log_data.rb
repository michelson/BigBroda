class LogData < ActiveRecord::Base
  establish_bq_connection  "bigquery"
end
