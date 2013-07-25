$:.push File.expand_path("../lib", __FILE__)
require "gearman/version"

Gem::Specification.new do |s|
  s.name          = %q{gearman-ruby}
  s.version       = Gearman::VERSION
  s.platform      = Gem::Platform::RUBY
  s.authors       = ["John Ewart", "Colin Curtin", "Daniel Erat", "Ladislav Martincik", "Pablo Delgado", "Mauro Pompilio", "Antonio Garrote", "Kim Altintop"]
  s.date          = %q{2013-07-25}
  s.summary       = %q{Ruby Gearman library}
  s.description   = %q{Library for the Gearman distributed job system}
  s.email         = %q{john@johnewart.net}
  s.homepage      = %q{http://github.com/johnewart/gearman-ruby}
  s.rubyforge_project = "gearman-ruby"

  s.extra_rdoc_files = [
    "LICENSE",
    "README",
    "TODO"
  ]

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  s.require_paths = ["lib"]
end

