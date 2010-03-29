require 'rake'
require 'rake/testtask'
require 'rake/rdoctask'
require 'rcov/rcovtask'
 
begin
  require 'jeweler'
  Jeweler::Tasks.new do |s|
    s.name = "xing-gearman-ruby"
    s.summary = "Library for the Gearman distributed job system"
    s.email = "ladislav.martincik@xing.com"
    s.homepage = "http://github.com/xing/gearman-ruby"
    s.description = "Library for the Gearman distributed job system"
    s.authors = ["Daniel Erat", "Ladislav Martincik", "Pablo Delgado", "Mauro Pompilio", "Antonio Garrote", "Kim Altintop"]
  end
rescue LoadError
  puts "Jeweler not available. Install it with: sudo gem install technicalpickles-jeweler -s http://gems.github.com"
end

Rake::TestTask.new do |t|
  t.libs << 'lib'
  t.pattern = 'test/**/*_test.rb'
  t.verbose = false
end

Rake::RDocTask.new do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = 'Gearman Ruby'
  rdoc.options << '--line-numbers' << '--inline-source'
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

Rcov::RcovTask.new do |t|
  t.libs << "test"
  t.test_files = FileList['test/*_test.rb']
  t.verbose = true
end

task :default => :rcov
