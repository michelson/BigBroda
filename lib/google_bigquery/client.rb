module GoogleBigquery
  class Client

    #CLIENT_OPTIONS = %w(api_method parameters ids start_date end_date dimensions metrics sort filters)
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

      #api_method = opts[:api_method] ||= @api.analytics.data.ga.get
      #start_date = opts[:start_date] ||= DateTime.now.prev_month.strftime("%Y-%m-%d")
      #end_date   = opts[:end_date]   ||= DateTime.now.strftime("%Y-%m-%d")
      #ids        = opts[:ids]        ||= "ga:#{GoogleBigquery::Config.profile_id}"
      #dimensions = opts[:dimensions] ||= "ga:day,ga:month"
      #metrics    = opts[:metrics]    ||= "ga:visits"
      #sort       = opts[:sort]       ||= "ga:month,ga:day"

      #self.api_method(api_method)
      #self.parameters({'ids' => ids,
      #                'start-date' => start_date,
      #                'end-date' => end_date,
      #                'dimensions' => dimensions,
      #                'metrics' => metrics,
      #                'sort' => sort })
    end

    def parse_response(res)
      JSON.parse(res.body)
    end

    def self.parse_response(res)
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

  end
end


