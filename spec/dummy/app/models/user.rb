class User < ActiveRecord::Base
  #establish_bq_connection "bigquery"
  has_many :posts
end
