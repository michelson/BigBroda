module GoogleBigquery
  class Client

    attr_accessor :options, :api, :client

    def initialize(opts = {})

      @api   ||= GoogleBigquery::Auth.api
      @client ||= GoogleBigquery::Auth.client
      #@auth.authorize # check expiration and cache ?

      self.tap do |client|
        client.options    ||= {}
        client.defaults_options(opts)
        client.options ||= opts
        yield client if block_given?
      end
    end

    def defaults_options(opts)

    end

    def parse_response(res)
      JSON.parse(res.body)
    end

    def self.parse_response(res)
      raise_detected_errors(res)
      JSON.parse(res.body)
    end

  private

    def merge_options(name, opts)
      @options.merge!  name => opts
    end

    def raise_detected_errors
      body = JSON.parse(@results.body)
      raise body["error"]["errors"].collect{|e| "#{e["reason"]}: #{e["message"]}" }.join(", ") if body.keys.include?("error")
    end

    def self.raise_detected_errors(res)
      body = JSON.parse(res.body)
      raise body["error"]["errors"].map{|o| "[BigQuery: #{o['domain']}]: #{o['reason']} #{o['message']}" }.join(", ") if body["error"].present?
      #raise body["error"]["errors"].collect{|e| "#{e["reason"]}: #{e["message"]}" }.join(", ") if body.keys.include?("error")
    end

  end
end


