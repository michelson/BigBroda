
module GoogleBigquery
  class Table < GoogleBigquery::Client

    attr_accessor :options 

    def initialize( opts={})
      super
    end

    def self.list(project_id, dataset_id)
      parse_response GoogleBigquery::Auth.client.execute( 
        GoogleBigquery::Auth.api.tables.list,
        projectId: project_id, datasetId: dataset_id
      )
    end

    def self.get(project_id, dataset_id, table_id)
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tables.get, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id, "tableId"=> table_id  }
      )
      parse_response(res)
    end

    def self.update(project_id, dataset_id, table_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tables.update, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id, "datasetId" => dataset_id, "tableId"=> table_id }
      )
      parse_response(res)
    end

    def self.patch(project_id, dataset_id, table_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tables.update, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id, "datasetId" => dataset_id, "tableId"=> table_id }
      )
      parse_response(res)
    end

    def self.create(project_id, dataset_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tables.insert, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id }
      )
      parse_response(res)
    end

    def self.delete(project_id, dataset_id, table_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tables.delete, 
        #:body_object=> body, #{"deleteContents"=> false}, 
        :parameters=> {"projectId"=> project_id, "datasetId" => dataset_id, "tableId"=> table_id }
      )
       res.status == 204 ?  true : parse_response(res)
    end

  end
end