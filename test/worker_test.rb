#!/usr/bin/env ruby
require 'rubygems'
require 'mocha'

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'

class TestClient < Test::Unit::TestCase
  ##
  # Test Gearman::Worker::Ability.
  def test_ability
    data, job = nil
    ability = Gearman::Worker::Ability.new(
      Proc.new {|d, j| data = d; job = j; true }, 5)
    assert_equal(5, ability.timeout)
    assert_equal(true, ability.run(1, 2))
    assert_equal(1, data)
    assert_equal(2, job)
  end

  ##
  # Test Gearman::Worker::Job.
  def test_job
    server = FakeJobServer.new(self)
    Thread.new do
      sock = TCPSocket.new('localhost', server.port)
      job = Gearman::Worker::Job.new(sock, 'handle')
      job.report_status(10, 20)
    end.run
    sock = server.expect_connection
    server.expect_request(sock, :work_status, "handle\00010\00020")
  end


  def test_job_reconnection
    server = FakeJobServer.new(self)
    Thread.new do
      sock = server.expect_connection
      server.expect_anything_and_close_socket(sock)
    end

    servers = ["localhost:#{server.port}"]
    w = Gearman::Worker.new(servers)
    Gearman::Util.stubs(:send_request).raises(Exception.new)
    w.add_ability('sleep') do |data,job|
      seconds = data
      (1..seconds.to_i).each do |i|
        sleep 1
        print i
        # Report our progress to the job server every second.
        job.report_status(i, seconds)
      end
      # Report success.
      true
    end

    assert(w.bad_servers.size == 1)
  end
end
