require 'spec_helper'
require 'socket'
require 'rspec'
require 'rspec/mocks'
require 'gearman'

describe Gearman::Util do
  before(:each) do

  end

  it "should generate a task from two arguments" do
    task = Gearman::Util.get_task_from_args("queue", "data")
    task.should_not be nil
  end

  it "should generate a task from three arguments" do
    task = Gearman::Util.get_task_from_args("queue", "data", {:background => true})
    task.should_not be nil
  end

  it "should raise an exception with more than three arguments" do
    expect {
      Gearman::Util.get_task_from_args("one", "two", {:three => :four}, :five)
    }.to raise_error
  end

  it "should raise a NetworkError when it didn't write as much as expected to a socket" do
    socket = double(TCPSocket)
    socket.should_receive(:write).with(anything).and_return(0)

    task = Gearman::Task.new("job_queue", "data")
    request = task.get_submit_packet
    expect {
      Gearman::Util.send_request(socket, request)
    }.to raise_error
  end

  context "normalizing job servers" do
    it "should handle a string for input" do
      Gearman::Util.normalize_job_servers("localhost:1234").should eq ["localhost:1234"]
    end

    it "should handle an array of host:port without changing a thing" do
      servers = ["localhost:123", "localhost:456"]
      Gearman::Util.normalize_job_servers(servers).should eq servers
    end

    it "should append the default port to anything in the array that doesn't have a port" do
      in_servers = ["foo.bar.com:123", "narf.quiddle.com"]
      out_servers = ["foo.bar.com:123", "narf.quiddle.com:4730"]
      Gearman::Util.normalize_job_servers(in_servers).should eq out_servers
    end
  end

  it "should convert a host:port & handle into its corresponding string" do
      Gearman::Util.handle_to_str("localhost:4730", "foo:1").should eq "localhost:4730//foo:1"
  end

  it "should convert a host:port & handle string into its components" do
    Gearman::Util.str_to_handle("localhost:4730//foo:1").should eq ["localhost:4730", "foo:1"]
  end

  it "should convert an ability name with prefix into its correct format" do
    Gearman::Util.ability_name_with_prefix("test", "a").should eq "test\ta"
  end
end