# GoogleBigquery

TODO: Write a gem description

## Installation

Add this line to your application's Gemfile:

    gem 'google_bigquery'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install google_bigquery

## Usage

TODO: Write usage instructions here

https://developers.google.com/bigquery/docs/reference/v2/datasets

https://developers.google.com/bigquery/docs/reference/v2/datasets#resource

Streaming data into BigQuery is free for an introductory period until January 1st, 2014. After that it will be billed at a flat rate of 1 cent per 10,000 rows inserted. The traditional jobs().insert() method will continue to be free. When choosing which import method to use, check for the one that best matches your use case. Keep using the jobs().insert() endpoint for bulk and big data loading. Switch to the new tabledata().insertAll() endpoint if your use case calls for a constantly and instantly updating stream of data.


## RESOURCES:

https://developers.google.com/bigquery/docs/authorization
https://developers.google.com/bigquery/client-libraries
https://developers.google.com/bigquery/loading-data-into-bigquery
https://developers.google.com/bigquery/streaming-data-into-bigquery
https://developers.google.com/apis-explorer/#s/bigquery/v2/
https://developers.google.com/bigquery/what-is-bigquery
https://developers.google.com/bigquery/docs/developers_guide

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
