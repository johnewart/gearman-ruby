#!/usr/bin/env ruby

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
end
