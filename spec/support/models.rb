

class GeneralModel < ActiveRecord::Base
  belongs_to :user
  belongs_to :data_import, class_name: "Importr::DataImport"
  validates :settings, presence: true
end

class User < ActiveRecord::Base
  has_many :general_models
end
