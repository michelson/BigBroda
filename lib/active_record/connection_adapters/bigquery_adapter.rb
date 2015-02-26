if ActiveRecord::VERSION::MAJOR == 4
  case ActiveRecord::VERSION::MINOR
  when 0
    require File.join(File.dirname(__FILE__), 'rails_41.rb')
  when 1
    require File.join(File.dirname(__FILE__), 'rails_41.rb')
  when 2
    require File.join(File.dirname(__FILE__), 'rails_42.rb')
  end
else
  raise "BigBroda only works on Rails 4.X version"
end