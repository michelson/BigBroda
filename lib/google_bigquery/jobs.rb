module GoogleBigquery 
  class Jobs < GoogleBigquery::Client

    def initialize(client=nil, opts={})
      @client = client
    end

    #query
    #Runs a BigQuery SQL query synchronously and returns query results if the query completes within a specified timeout. 
    def self.query(project_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.query, 
        :body_object=> body, 
        :parameters=> {"projectId"=> project_id}
      )
      parse_response(res)

    end

    #Retrieves the specified job by ID.
    def self.get(project_id , job_id)
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.get, 
        :parameters=> {"projectId"=> project_id, "jobId"=>job_id}
      )
      parse_response(res)
    end

    #Retrieves the results of a query job.
    def self.getQueryResults(project_id , job_id, params={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.get_query_results, 
        :parameters=> {"projectId"=> project_id, "jobId"=>job_id}.merge(params)
      )
      parse_response(res)
    end

    #Starts a new asynchronous job.
    def self.insert(project_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.insert, 
        :body_object=> body, 
        :parameters=> {"projectId"=> project_id}
      )
      parse_response(res)
    end

    #Lists all the Jobs in the specified project that were started by the user.
    def self.list(project_id, params={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.list, 
        :parameters=> {"projectId"=> project_id}.merge(params)
      )
      parse_response(res)
    end

    #export data
    #TODO: get mappings for formatting options
    def self.export(project_id, dataset_id, table_id, bucket_location)
      body = {'projectId'=> project_id,
       'configuration'=> {
        'extract'=> {
          'sourceTable'=> {
             'projectId'=> project_id,
             'datasetId'=> dataset_id,
             'tableId'=> table_id
           },
          'destinationUri'=> "gs://#{bucket_location}",
          'destinationFormat'=> 'NEWLINE_DELIMITED_JSON'
         }
       }
      }

      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.insert, 
        :body_object=> body, 
        :parameters=> {"projectId"=> project_id}
      )
   
      job_id = JSON.parse(res.body)["jobReference"]["jobId"]
      puts 'Waiting for export to complete..'

      loop do 
        status = JSON.parse(self.get(project_id, job_id).body)

        if 'DONE' == status['status']['state']

          puts "Done exporting!"
          if status["status"]["errors"]
            puts status["status"]["errors"].map{|o| "#{o['reason']} : #{o['message']}"}
          end

          return

        end
        sleep(10)
      end
    end

    #TODO: get mappings for formatting options
    def self.load(project_id, dataset_id, table_id, sources, fields)
      body = { 'projectId'=> project_id,
       'configuration'=> {
        'load'=> {
          'sourceFormat' => "NEWLINE_DELIMITED_JSON",
          'sourceUri' => sources.first,
          'sourceUris' => sources, 
          
          'destinationTable'=> {
            'projectId'=> project_id,
            'datasetId'=> dataset_id,
            'tableId'=> table_id
          }
         }
       }
      }
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.jobs.insert, 
        :body_object=> body, 
        :parameters=> {"projectId"=> project_id}
      )
      #binding.pry
      job_id = JSON.parse(res.body)["jobReference"]["jobId"]
      puts 'Waiting for import to complete..'
      
      loop do 
        status = JSON.parse(self.get(project_id, job_id).body)

        if 'DONE' == status['status']['state']

          puts "Done loading!"
          if status["status"]["errors"]
            puts status["status"]["errors"].map{|o| "#{o['reason']} : #{o['message']}"}
          end

          return

        end
        sleep(10)
      end
    end

    def self.import()
    end

    def self.copy()
    end

    private

    def self.build_body_object(options)
      project_id = options[:project_id] 
      dataset_id = options[:dataset_id]
      table_id   = options[:table_id]
      bucket_location = options[:bucket_location]
      {'projectId'=> project_id,
       'configuration'=> {
        'extract'=> {
          'sourceTable'=> {
             'projectId'=> project_id,
             'datasetId'=> dataset_id,
             'tableId'=> table_id
           },
          'destinationUri'=> "gs://#{bucket_location}",
          'destinationFormat'=> 'NEWLINE_DELIMITED_JSON'
         }
       }
      }
    end

  end
end
