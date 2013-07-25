require 'spec_helper'
require 'socket'
require 'rspec'
require 'rspec/mocks'
require 'gearman'

describe Gearman::Client do
  before(:all) do
    @tcp_server = TCPServer.new 5789
    @client = Gearman::Client.new(["localhost:5789"])
  end

  after(:all) do
    @tcp_server.close
  end

  it "creates a client" do
    @client.should_not be nil
  end

  it "creates a task set when you run a task" do
    task_set = Gearman::TaskSet.new(@client)
    Gearman::TaskSet.stub(:new).and_return task_set
    task_set.should_receive(:add_task).and_return true
    task_set.should_receive(:wait)

    task = Gearman::Task.new("do_something", {:data => 123})
    @client.do_task(task)
  end

  it "raises an exception when submitting a job fails" do
    task = Gearman::Task.new("queue", "data")
    @client.should_receive(:get_job_server).and_raise Gearman::NoJobServersError
    expect {
      @client.do_task(task)
    }.to raise_error
  end

  it "gets a socket for the client's host:port combo" do
    sock = @client.get_socket("localhost:5789")
    sock.should_not be nil
  end

  it "closes sockets it doesn't know about when asked to return them" do
    sock = double(TCPSocket)
    sock.should_receive(:addr).and_return [nil, 1234, 'hostname', '1.2.3.4']
    sock.should_receive(:close)
    @client.return_socket(sock)
  end

  it "properly emits an options request" do
    Gearman::Util.should_receive(:send_request)
    Gearman::Util.should_receive(:read_response).and_return([:error, "Snarf"])
    expect {
      @client.option_request("exceptions")
    }.to raise_error

  end

end
