require File.expand_path(File.dirname(__FILE__) + '../../spec_helper')


describe "Client" do
  before :each do
    config_setup
  end


  describe "errors" do
    before :each do
      @error_report = GoogleBigquery::Client.new()
      @error_report.parameters({'ids' => "ga:#{GoogleBigquery::Config.profile_id}",
                                'start-date' => DateTime.now.prev_month.strftime("%Y-%m-%d"),
                                'end-date' => DateTime.now.strftime("%Y-%m-%d"),
                                'dimensions' => "ga:year,ga:month",
                                'metrics' => "ga:visits,ga:bounces,ga:entranceBounceRate",
                                'sort' => "ga:month,ga:day" })

    end

    it "should raise and error" do
      lambda { @error_report.results }.should raise_error
    end
  end

  describe "override entire options" do
    before :each do
      @report = GoogleBigquery::Client.new()
      @report.parameters({'ids'        => "ga:123456",
                          'dimensions' => "ga:year,ga:month",
                          'sort'       => "ga:month,ga:day",
                          'quotaUser'  => "MySpecialUser" })

    end

    it "options should have new options" do
      @report.options[:parameters].keys.should == ["ids", "dimensions", "sort"]
      @report.options[:parameters]["ids"].should == "ga:123456"
    end

  end

  describe "initialize with specific options without overriding defaults" do
    before :each do
      @report = GoogleBigquery::Client.new({:ids => "ga:123"})
    end

    it "options should have new options" do
      @report.options[:parameters].keys.should == ["ids", "start-date", "end-date", "dimensions", "metrics", "sort"]
      @report.options[:parameters]["ids"].should == "ga:123"
    end

  end

  describe "block initialization" do
    before :each do
      @report = GoogleBigquery::Client.new do |client|
        client.parameters({ "ids" => "ga:123456"})
      end
    end
    it "options should have new options" do
      @report.options[:parameters].keys.should == ["ids"]
      @report.options[:parameters]["ids"].should == "ga:123456"
    end
  end

  describe "Client" do
    before(:all) do
      @client = GoogleBigquery::Client.new()
    end

    it "find objects object" do
      @client.results.should_not be_nil
    end

    it "headers raw" do
      @client.results.data.column_headers[0]["name"].should == "ga:day"
      @client.results.data.column_headers[1]["name"].should == "ga:month"
      @client.results.data.column_headers[2]["name"].should == "ga:visits"
    end

    it "rows raw" do
      @client.results.data.rows.class.should be Array
      @client.results.data.rows.should_not be_empty
    end

    it "headers" do
      @client.formatted_columns.should == "ga:day\tga:month\tga:visits"
    end

    it "rows" do
      @client.formatted_rows.class.should == Array
      @client.formatted_rows.should_not be_empty
    end

  end

end
