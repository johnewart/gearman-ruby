Given /^gearmand is running$/ do
end

Given /^a basic worker is running$/ do
  gearman = Gearman::Server.new('localhost:4730')
  worker  = Gearman::Worker.new('localhost:4730')
  worker.add_ability('sleep')
  assert worker.status == "preparing".to_sym
  assert gearman.status.empty?
end

When /^I add a basic client$/ do
  pending # express the regexp above with the code you wish you had
end

Then /^the job should be processed$/ do
  pending # express the regexp above with the code you wish you had
end

Then /^I should see "([^"]*)"$/ do |arg1|
  pending # express the regexp above with the code you wish you had
end
