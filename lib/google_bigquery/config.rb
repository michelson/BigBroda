require "active_support/core_ext/module/attribute_accessors"

module GoogleBigquery
  class Config

    def self.setup
      yield self
    end

    mattr_accessor  :pass_phrase,
                    :key_file,
                    :client_id,
                    :scope,
                    :profile_id,
                    :email
  end
end