#!/usr/bin/env ruby

$:.unshift('../lib')
require './testlib'
require 'gearman'
require 'test/unit'
require 'thread'

Thread.abort_on_exception = true

class TestClient < Test::Unit::TestCase
  def test_client
    server = FakeJobServer.new(self)
    client, task1, task2, taskset, sock, res1, res2 = nil

    s = TestScript.new
    c = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new("localhost:#{server.port}") }

    c.exec { task1 = Gearman::Task.new('add', '5 2') }
    c.exec { task1.on_complete {|d| res1 = d.to_i } }
    c.exec { taskset = Gearman::TaskSet.new(client) }
    c.exec { taskset.add_task(task1) }
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :submit_job, "add\000\0005 2") }
    s.exec { server.send_response(sock, :job_created, "a") }

    # Create a second task.  It should use the same socket as the first.
    c.exec { task2 = Gearman::Task.new('add', '10 5') }
    c.exec { task2.on_complete {|d| res2 = d.to_i } }
    c.exec { taskset.add_task(task2) }

    # Return the response to the first job before the handle for the
    # second.
    s.exec { server.send_response(sock, :work_complete, "a\0007") }
    s.exec { server.expect_request(sock, :submit_job, "add\000\00010 5") }
    s.exec { server.send_response(sock, :job_created, "b") }

    # After the client waits on the taskset, send the response to the
    # second job.
    c.exec { taskset.wait }
    s.exec { server.send_response(sock, :work_complete, "b\00015") }
    c.wait

    # Check that we got the right answers.
    assert_equal(7, res1)
    assert_equal(15, res2)
  end
end
