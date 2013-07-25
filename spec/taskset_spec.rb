require 'spec_helper'
require 'socket'
require 'rspec'
require 'rspec/mocks'
require 'gearman'

describe Gearman::TaskSet do
  before do

  end

  after do

  end

  it "handles a NetworkError when submitting a job" do
    bad_socket = double(TCPSocket)
    bad_socket.should_receive(:write) { |*args|
      args[0].length
    }.at_least(:once)
    bad_socket.should_receive(:close)

    good_socket = double(TCPSocket)
    good_socket.should_receive(:write) { |*args|
      args[0].length
    }.at_least(:once)

    Gearman::Util.should_receive(:timed_recv).with(bad_socket, 12, anything).and_raise Gearman::NetworkError
    Gearman::Util.should_receive(:timed_recv).with(good_socket, 12, anything).and_return("\x00RES\x00\x00\x00\x08\000\000\000\007")
    Gearman::Util.should_receive(:timed_recv).with(good_socket, 7, anything).and_return("foo:123")

    client = Gearman::Client.new(["localhost:4731", "localhost:4732"])
    client.should_receive(:get_hostport_for_socket).at_least(1).times.with(bad_socket).and_return "localhost:4731"
    client.should_receive(:get_hostport_for_socket).at_least(1).times.with(good_socket).and_return "localhost:4732"
    client.should_receive(:get_socket).with("localhost:4731").and_return bad_socket
    client.should_receive(:get_socket).with("localhost:4732").and_return good_socket

    task = Gearman::Task.new("job_queue", "data")

    task_set = Gearman::TaskSet.new(client)
    task_set.add_task(task)
  end

  it "waits for an answer from the server" do
    good_socket = double(TCPSocket)
    good_socket.should_receive(:write) { |*args|
      args[0].length
    }.at_least(:once)

    Gearman::Util.should_receive(:timed_recv).with(good_socket, 12, anything).and_return("\x00RES\x00\x00\x00\x08\000\000\000\007")
    Gearman::Util.should_receive(:timed_recv).with(good_socket, 7, anything).and_return("foo:123")

    client = Gearman::Client.new(["localhost:4731"])
    client.should_receive(:get_hostport_for_socket).at_least(1).times.with(good_socket).and_return "localhost:4731"
    client.should_receive(:get_socket).with("localhost:4731").and_return good_socket

    task = Gearman::Task.new("job_queue", "data")

    task_set = Gearman::TaskSet.new(client)
    task_set.add_task(task)

    Gearman::Util.should_receive(:timed_recv).with(good_socket, 12, anything) {
      sleep 0.5
      "\x00RES\x00\x00\x00\x0d\000\000\000\007"
    }

    Gearman::Util.should_receive(:timed_recv).with(good_socket, 7, anything) {
      "foo:123"
    }

    IO.stub(:select).and_return([[good_socket]])
    start_time = Time.now
    task_set.wait(30)
    time_diff = Time.now - start_time
    time_diff.should be < 30
    time_diff.should be > 0
  end

end