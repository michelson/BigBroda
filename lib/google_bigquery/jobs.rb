module GoogleBigquery 
  class Jobs < GoogleBigquery::Client

    def initialize(client=nil, opts={})
      @client = client
    end

    def self.query(project_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.query, 
        :body_object=> body, 
        :parameters=> {"projectId"=> project_id}
      )
      parse_response(res)

    end
  end
end