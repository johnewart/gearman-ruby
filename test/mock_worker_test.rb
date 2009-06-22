#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'
require 'thread'

Thread.abort_on_exception = true

class TestWorker < Test::Unit::TestCase
  def test_complete
    server = FakeJobServer.new(self)
    worker = nil
    sock = nil

    s = TestScript.new
    w = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    worker_thread = Thread.new { w.loop_forever }.run

    # Create a worker and wait for it to connect to us.
    w.exec {
      worker = Gearman::Worker.new(
        "localhost:#{server.port}", nil, { :client_id => 'test' })
    }
    s.exec { sock = server.expect_connection }
    s.wait

    # After it connects, it should send its ID, and it should tell us its
    # abilities when we report them.
    s.exec { server.expect_request(sock, :set_client_id, 'test') }
    w.exec { worker.add_ability('echo') {|d,j| j.report_status(1, 1); d } }
    s.exec { server.expect_request(sock, :can_do, 'echo') }

    # It should try to grab a job when we tell it to work.
    w.exec { worker.work }
    s.exec { server.expect_request(sock, :grab_job) }

    # If we tell it there aren't any jobs, it should go to sleep.
    s.exec { server.send_response(sock, :no_job) }
    s.exec { server.expect_request(sock, :pre_sleep) }

    # When we send it a noop, it should wake up and ask for a job again.
    s.exec { server.send_response(sock, :noop) }
    s.exec { server.expect_request(sock, :grab_job) }

    # When we give it a job, it should do it.
    s.exec { server.send_response(sock, :job_assign, "a\0echo\0foo") }
    s.exec { server.expect_request(sock, :work_status, "a\0001\0001") }
    s.exec { server.expect_request(sock, :work_complete, "a\0foo") }

    # Test that functions are unregistered correctly.
    w.exec { worker.remove_ability('echo') }
    s.exec { server.expect_request(sock, :cant_do, 'echo') }
    s.wait
  end

  def test_multiple_servers
    server1 = FakeJobServer.new(self)
    server2 = FakeJobServer.new(self)
    # This is cheesy.  We want to know the order that Worker#work will
    # iterate through the servers, so we make sure that server1 will be the
    # first one when the names are lexographically sorted.
    if server2.port.to_s < server1.port.to_s
      tmp = server1
      server1 = server2
      server2 = tmp
    end
    worker = nil
    sock1, sock2 = nil

    s1 = TestScript.new
    s2 = TestScript.new
    w = TestScript.new

    server1_thread = Thread.new { s1.loop_forever }.run
    server2_thread = Thread.new { s2.loop_forever }.run
    worker_thread = Thread.new { w.loop_forever }.run

    # Create a worker, which should connect to both servers.
    w.exec {
      worker = Gearman::Worker.new(
        nil, nil, { :client_id => 'test', :reconnect_sec => 0.1 }) }
    w.exec { worker.add_ability('foo') {|d,j| 'bar' } }
    w.exec {
      worker.job_servers =
        [ "localhost:#{server1.port}", "localhost:#{server2.port}" ]
    }
    s1.exec { sock1 = server1.expect_connection }
    s2.exec { sock2 = server2.expect_connection }
    s1.wait
    s2.wait

    # It should register itself with both.
    s1.exec { server1.expect_request(sock1, :set_client_id, 'test') }
    s1.exec { server1.expect_request(sock1, :can_do, 'foo') }
    s2.exec { server2.expect_request(sock2, :set_client_id, 'test') }
    s2.exec { server2.expect_request(sock2, :can_do, 'foo') }

    # It should try to get a job from both servers and then sleep.
    w.exec { worker.work }
    s1.exec { server1.expect_request(sock1, :grab_job) }
    s1.exec { server1.send_response(sock1, :no_job) }
    s2.exec { server2.expect_request(sock2, :grab_job) }
    s2.exec { server2.send_response(sock2, :no_job) }
    s1.exec { server1.expect_request(sock1, :pre_sleep) }
    s2.exec { server2.expect_request(sock2, :pre_sleep) }

    # If the second server wakes it up, it should again try to get a job
    # and then do it.
    s2.exec { server2.send_response(sock2, :noop) }
    s1.exec { server1.expect_request(sock1, :grab_job) }
    s1.exec { server1.send_response(sock1, :no_job) }
    s2.exec { server2.expect_request(sock2, :grab_job) }
    s2.exec { server2.send_response(sock2, :job_assign, "a\0foo\0") }
    s2.exec { server2.expect_request(sock2, :work_complete, "a\0bar") }

    w.wait
    s1.wait
    s2.wait

    # Stop the first job server and make the worker try to reconnect to
    # both.
    old_servers = worker.job_servers
    server1.stop
    worker.job_servers = []
    worker.job_servers = old_servers
    s2.exec { sock2 = server2.expect_connection }
    s2.wait

    # It shouldn't have any trouble with the second server.  Tell it to go
    # to work.
    s2.exec { server2.expect_request(sock2, :set_client_id, 'test') }
    s2.exec { server2.expect_request(sock2, :can_do, 'foo') }
    w.exec { worker.work }
    s2.exec { server2.expect_request(sock2, :grab_job) }
    s2.exec { server2.send_response(sock2, :no_job) }
    s2.exec { server2.expect_request(sock2, :pre_sleep) }
    s2.wait

    # Start the first server and wait for the worker to connect to it and
    # register.
    server1.start
    s1.exec { sock1 = server1.expect_connection }
    s1.wait
    s1.exec { server1.expect_request(sock1, :set_client_id, 'test') }
    s1.exec { server1.expect_request(sock1, :can_do, 'foo') }
    s1.wait

    # Let the second server wake the worker up and then give it a job.
    s2.exec { server2.send_response(sock2, :noop) }
    s1.exec { server1.expect_request(sock1, :grab_job) }
    s1.exec { server1.send_response(sock1, :no_job) }
    s2.exec { server2.expect_request(sock2, :grab_job) }
    s2.exec { server2.send_response(sock2, :job_assign, "a\0foo\0") }
    s2.exec { server2.expect_request(sock2, :work_complete, "a\0bar") }
    s1.wait
    s2.wait
    w.wait
  end

  def test_timeout
    server = FakeJobServer.new(self)
    worker = nil
    sock = nil

    s = TestScript.new
    w = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    worker_thread = Thread.new { w.loop_forever }.run

    w.exec {
      worker = Gearman::Worker.new("localhost:#{server.port}", nil,
        { :client_id => 'test',
          :reconnect_sec => 0.15,
          :network_timeout_sec => 0.1 })
    }
    s.exec { sock = server.expect_connection }
    s.wait
    s.exec { server.expect_request(sock, :set_client_id, 'test') }

    w.exec { worker.add_ability('foo') {|d,j| 'bar' } }
    s.exec { server.expect_request(sock, :can_do, 'foo') }

    # Don't do anything after the client asks for a job.
    w.exec { worker.work }
    s.exec { server.expect_request(sock, :grab_job) }
    s.exec { sleep 0.16 }
    s.wait

    # The client should reconnect and ask for a job again.
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :set_client_id, 'test') }
    s.exec { server.expect_request(sock, :can_do, 'foo') }
    s.exec { server.expect_request(sock, :grab_job) }
    s.exec { server.send_response(sock, :job_assign, "a\0foo\0") }
    s.exec { server.expect_request(sock, :work_complete, "a\0bar") }
    s.wait
    w.wait
  end
end
