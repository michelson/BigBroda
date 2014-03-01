
module GoogleBigquery
  class TableData < GoogleBigquery::Client

    def self.create(project_id, dataset_id, table_id, body={})
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tabledata.insert_all, 
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}}, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id, "tableId"=>table_id }
      )
      parse_response(res)
    end

    def self.list(project_id, dataset_id, table_id)
      res = GoogleBigquery::Auth.client.execute(
        :api_method=> GoogleBigquery::Auth.api.tabledata.list, 
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id, "tableId"=>table_id }
      )
      parse_response(res)
    end

  end
end