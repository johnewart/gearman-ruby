require 'spec_helper'
require 'socket'

describe Gearman::Client do
  before(:all) do
    @tcp_server = TCPServer.new 5789
  end

  after(:all) do
    @tcp_server.close
  end

  before(:each) do
    @mock_connection_pool = double(Gearman::ConnectionPool)
    Gearman::ConnectionPool.stub(:new).and_return @mock_connection_pool

    @client = Gearman::Client.new(["localhost:5789"])
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
    @mock_connection_pool.should_receive(:get_connection).and_raise Gearman::NoJobServersError
    expect {
      @client.do_task(task)
    }.to raise_exception
  end

  it "properly emits an options request" do
    mock_connection = double(Gearman::Connection)
    mock_connection.should_receive(:send_request).and_return([:error, "Snarf"])

    @mock_connection_pool.should_receive(:with_all_connections).and_yield mock_connection

    expect {
      @client.set_options("exceptions")
    }.to raise_error

  end



  it "should raise a NetworkError when it didn't write as much as expected to a socket" do
    socket = double(TCPSocket)
    socket.should_receive(:write).with(anything).and_return(0)

    task = Gearman::Task.new("job_queue", "data")
    request = task.get_submit_packet
    connection = Gearman::Connection.new("localhost", 1234)
    connection.should_receive(:socket).and_return socket

    expect {
      connection.send_request(request)
    }.to raise_error
  end



end
