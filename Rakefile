require "bundler/gem_tasks"

desc 'Default: run unit specs.'
task :default => :spec

desc 'Test the BQ plugin.'
RSpec::Core::RakeTask.new('spec') do |t|
    t.pattern = FileList['spec/**/*_spec.rb']
end

