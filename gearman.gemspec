require 'rubygems'
SPEC = Gem::Specification.new do |s|
  s.name          = "gearman"
  s.version       = "0.0.1"
  s.author        = "Daniel Erat"
  s.email         = "dan-ruby@erat.org"
  s.homepage      = "http://www.erat.org/ruby/"
  s.platform      = Gem::Platform::RUBY
  s.summary       = "Library for the Gearman distributed job system"
  candidates      = Dir.glob("{*,{lib{,/gearman},test}/*}")
  s.files         = candidates.delete_if {|i| i =~ /.svn/ }
  s.require_path  = "lib"
  s.autorequire   = "gearman"
  s.test_files    = Dir.glob("test/*.rb")
  s.has_rdoc      = true
end
