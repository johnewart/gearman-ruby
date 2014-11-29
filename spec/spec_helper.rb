require 'simplecov'
require 'rspec'
require 'rspec/mocks'

SimpleCov.start do
  add_filter "/spec/"
  merge_timeout 3600
end

$:.unshift(File.expand_path('../lib', __FILE__))
require 'gearman'

Gearman.logger = Logger.new(STDERR)
Gearman.logger.level = Logger::DEBUG

RSpec.configure do |config|
  config.mock_with :rspec
end
