module GoogleBigquery
  class Jobs

    def initialize(client=nil, opts={})
      @client = client
    end

    def query
      @auth.client.execute(
        :api_method=> @auth.api.jobs.query, 
        :body_object=> {"query"=> "SELECT 17"}, 
        :parameters=> {:"projectId"=> "1234"}
      )
    end
  end
end