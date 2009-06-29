# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{gearman-ruby}
  s.version = "1.0.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Daniel Erat", "Ladislav Martincik"]
  s.date = %q{2009-06-25}
  s.description = %q{Library for the Gearman distributed job system}
  s.email = %q{ladislav.martincik@xing.com}
  s.extra_rdoc_files = [
    "LICENSE",
     "README"
  ]
  s.files = [
    ".gitignore",
     "LICENSE",
     "README",
     "Rakefile",
     "VERSION.yml",
     "examples/Net/Gearman/Client.php",
     "examples/Net/Gearman/Connection.php",
     "examples/Net/Gearman/Exception.php",
     "examples/Net/Gearman/Job.php",
     "examples/Net/Gearman/Job/Common.php",
     "examples/Net/Gearman/Job/Exception.php",
     "examples/Net/Gearman/Job/Sleep.php",
     "examples/Net/Gearman/Manager.php",
     "examples/Net/Gearman/Set.php",
     "examples/Net/Gearman/Task.php",
     "examples/Net/Gearman/Worker.php",
     "examples/client.php",
     "examples/client.rb",
     "examples/scale_image.rb",
     "examples/scale_image_worker.rb",
     "examples/server.rb",
     "examples/worker.php",
     "examples/worker.rb",
     "gearman-ruby.gemspec",
     "lib/gearman.rb",
     "lib/gearman/client.rb",
     "lib/gearman/server.rb",
     "lib/gearman/task.rb",
     "lib/gearman/taskset.rb",
     "lib/gearman/testlib.rb",
     "lib/gearman/util.rb",
     "lib/gearman/worker.rb",
     "test/client_test.rb",
     "test/mock_client_test.rb",
     "test/mock_worker_test.rb",
     "test/worker_test.rb"
  ]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/xing/gearman-ruby}
  s.rdoc_options = ["--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.1}
  s.summary = %q{Library for the Gearman distributed job system}
  s.test_files = [
    "test/client_test.rb",
     "test/mock_client_test.rb",
     "test/mock_worker_test.rb",
     "test/worker_test.rb",
     "examples/client.rb",
     "examples/scale_image.rb",
     "examples/scale_image_worker.rb",
     "examples/server.rb",
     "examples/worker.rb"
  ]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
