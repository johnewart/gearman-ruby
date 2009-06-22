#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'

class TestClient < Test::Unit::TestCase
  ##
  # Test the get_socket, return_socket, close_socket, and
  # get_hostport_for_socket methods of Client.
  def test_sockets
    server = FakeJobServer.new(self)
    client = Gearman::Client.new
    hostport = "localhost:#{server.port}"
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
end
