module GoogleBigquery
  class Auth

    attr_accessor :api, :client
    cattr_accessor :api, :client

    def initialize
      config = GoogleBigquery::Config
      @key = Google::APIClient::PKCS12.load_key(config.key_file, config.pass_phrase)
      @asserter = Google::APIClient::JWTAsserter.new( config.email, config.scope, @key)
    end

    def authorize
      @client = Google::APIClient.new()
      @client.authorization = @asserter.authorize()
      @api = @client.discovered_api("bigquery",'v2')
      self.class.api = @api
      self.class.client = @client
    end

  end
end

