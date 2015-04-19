
module BigBroda
  class TableData < BigBroda::Client

    def self.create(project_id, dataset_id, table_id, body={})
      res = BigBroda::Auth.client.execute(
        :api_method=> BigBroda::Auth.api.tabledata.insert_all,
        :body_object=> body , #{"datasetReference"=> {"datasetId" =>"whoa"}},
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id, "tableId"=>table_id }
      )
      parse_response(res)
    end

    def self.list(project_id, dataset_id, table_id)
      res = BigBroda::Auth.client.execute(
        :api_method=> BigBroda::Auth.api.tabledata.list,
        :parameters=> {"projectId"=> project_id, "datasetId"=> dataset_id, "tableId"=>table_id }
      )
      parse_response(res)
    end

  end
end