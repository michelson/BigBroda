
module GoogleBigquery
  class Dataset < GoogleBigquery::Client

    attr_accessor :options 

    def initialize( opts={})
      super
    end

    def self.list(project_id)
      parse_response GoogleBigquery::Auth.client.execute( 
        GoogleBigquery::Auth.api.datasets.list,
        projectId: project_id
      )
    end

    def self.get(project_id, dataset_id)
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.datasets.get, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id }
      )
      parse_response(res)
    end

    def self.update(project_id, dataset_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.datasets.update, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id }
      )
      parse_response(res)
    end

    def self.patch(project_id, dataset_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.datasets.update, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id }
      )
      parse_response(res)
    end

    def self.create(project_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.datasets.insert, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id }
      )
      parse_response(res)
    end

    def delete(opts)
    end

    def self.delete(project_id, dataset_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.datasets.delete, 
        #:body_object=> {"deleteContents"=> true}, 
        :parameters=> {"projectId"=> project_id, "datasetId" => dataset_id }
      )
       res.status == 204 ?  true : parse_response(res)
    end

  end
end
#Path parameters
#projectId   string  Project ID of the datasets to be listed
#Optional query parameters
#all   boolean   Whether to list all datasets, including hidden ones
#maxResults  unsigned integer  The maximum number of results to return
#pageToken   string  Page token, returned by a previous call, to request the next page of results 