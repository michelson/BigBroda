
module BigBroda
  class Project < BigBroda::Client

    attr_accessor :options

    def initialize( opts={})
      super
    end

    def self.list
      parse_response BigBroda::Auth.client.execute( BigBroda::Auth.api.projects.list)
    end

  end
end