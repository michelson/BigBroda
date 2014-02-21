
module GoogleBigquery
  class Project < GoogleBigquery::Client

    attr_accessor :options 

    def initialize( opts={})
      super
    end

    def self.list
      parse_response GoogleBigquery::Auth.client.execute( GoogleBigquery::Auth.api.projects.list)
    end

  end
end