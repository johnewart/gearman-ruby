# Tell cucumber I'm using ruby
$LOAD_PATH << File.expand_path('../../../lib', __FILE__)
require 'gearman'

require 'test/unit'

World do
  include Test::Unit::Assertions
end