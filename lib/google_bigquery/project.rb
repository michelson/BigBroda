
module GoogleBigquery
  class Project

    attr_accessor :client

    def list
      parse_response @client.client.execute( @client.api.projects.list)
    end

  end
end