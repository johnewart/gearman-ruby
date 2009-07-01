#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'

class TestClient < Test::Unit::TestCase

  def setup
    @server = FakeJobServer.new(self)
    @server_b = FakeJobServer.new(self)
  end

  def teardown
    @server.stop
  end
  ##
  # Test the get_socket, return_socket, close_socket, and
  # get_hostport_for_socket methods of Client.
  def test_sockets
    client = Gearman::Client.new
    hostport = "localhost:#{@server.port}"
    client.job_servers = [hostport]

    # If we get a socket, return it, and request a socket for the same
    # host, we should get the original one again.
    origsock = client.get_socket(client.get_job_server)
    client.return_socket(origsock)
    assert_equal(origsock, client.get_socket(client.get_job_server))
    assert_equal(hostport, client.get_hostport_for_socket(origsock))

    # We should get another socket if we call get_socket again.
    newsock = client.get_socket(client.get_job_server)
    assert_not_equal(origsock, newsock)
    assert_equal(hostport, client.get_hostport_for_socket(newsock))

    # They should be closed when we call close_socket, and we should get
    # a new socket the next time.
    client.close_socket(origsock)
    client.close_socket(newsock)
    assert(origsock.closed?)
    assert(newsock.closed?)
    assert_not_equal(origsock, client.get_socket(client.get_job_server))
  end

  ##
  # We check that the client does not fail if at least one
  # server is up.
  def test_first_connection_server_down_retry
    client = Gearman::Client.new
    hostport_good = "localhost:#{@server.port}"
    hostport_bad = "nonexistent:8080"
    # TODO: why does this fail?
    # hostport_bad = "localhost:#{@server.port+x}" for x in (1..5)

    client.job_servers = [hostport_bad,hostport_good]

    first_requested_server = client.get_job_server
    assert(first_requested_server == hostport_bad)

    begin
      result = client.get_socket(first_requested_server)
      assert(false)
    rescue RuntimeError
      assert(true)
    end

    assert(client.bad_servers.size == 1)

    second_requested_server = client.get_job_server
    assert(second_requested_server == hostport_good)
    begin
      client.get_socket(second_requested_server)
      assert(true)
    rescue RuntimeError
      assert(false)
    end
  end

  ##
  # We check that the client raises a fatal exception if server fails
  # while connected.
  def test_client_down_if_server_down
    client = Gearman::Client.new
    hostport_good = "localhost:#{@server.port}"
    hostport_good_b = "localhost:#{@server_b.port}"

     client.job_servers = [hostport_good,hostport_good_b]

    # We simulate a gearmand server failure after receiving some data
    # from the client
    Thread.new do
      server_socket = @server.expect_connection
      @server.expect_anything_and_close_socket(server_socket)
    end

    taskset = Gearman::TaskSet.new(client)

    task = Gearman::Task.new('sleep', 20)
    task.on_complete {|d| puts d }

    should_be_true = true
    begin
      taskset.add_task(task)
      should_be_true = false
    rescue Exception => ex
    end
    assert(should_be_true)
  end

  def test_option_request_exceptions
    this_server = FakeJobServer.new(self)
    Thread.new do
      server_socket = this_server.expect_connection
      this_server.expect_request(server_socket, "option_req", "exceptions")
      this_server.send_response(server_socket, :job_created, 'a')
    end
    client = Gearman::Client.new
    hostport = "localhost:#{this_server.port}"
    client.job_servers = [hostport]
    client.option_request("exceptions")
  end

  def test_option_request_bad
    this_server = FakeJobServer.new(self)
    Thread.new do
      server_socket = this_server.expect_connection
      this_server.expect_request(server_socket, "option_req", "cccceptionsccc")
      this_server.send_response(server_socket, :exception, 'a')
    end

    client = Gearman::Client.new
    hostport = "localhost:#{this_server.port}"
    client.job_servers = [hostport]
    begin
      client.option_request("cccceptionsccc")
      assert(false)
    rescue Gearman::ProtocolError
      assert(true)
    end
  end


end
