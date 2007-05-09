#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'test/unit'
require './testlib'
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
  end
end
