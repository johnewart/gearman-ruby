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
    s.exec { server.send_response(sock, :job_created, 'a') }

    # Create a second task.  It should use the same socket as the first.
    c.exec { task2 = Gearman::Task.new('add', '10 5') }
    c.exec { task2.on_complete {|d| res2 = d.to_i } }
    c.exec { taskset.add_task(task2) }

    # Return the response to the first job before the handle for the
    # second.
    s.exec { server.send_response(sock, :work_complete, "a\0007") }
    s.exec { server.expect_request(sock, :submit_job, "add\000\00010 5") }
    s.exec { server.send_response(sock, :job_created, 'b') }

    # After the client waits on the taskset, send the response to the
    # second job.
    c.exec { taskset.wait }
    s.exec { server.send_response(sock, :work_complete, "b\00015") }
    c.wait

    # Check that we got the right answers.
    assert_equal(7, res1)
    assert_equal(15, res2)
  end

  ##
  # Test that Gearman::Task's callback's get called when they should.
  def test_callbacks
    server = FakeJobServer.new(self)
    client, task, taskset, sock = nil
    failed, retries, num, den = nil

    s = TestScript.new
    c = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new("localhost:#{server.port}") }

    task = Gearman::Task.new('foo', 'bar',
      { :retry_count => 2 })
    task.on_fail { failed = true }
    task.on_retry {|r| retries = r }
    task.on_status {|n,d| num = n.to_i; den = d.to_i }

    c.exec { taskset = Gearman::TaskSet.new(client) }
    c.exec { taskset.add_task(task) }
    s.exec { sock = server.expect_connection }
    s.wait

    # Send three failures back to the client.
    c.exec { taskset.wait }
    s.exec { server.expect_request(sock, :submit_job, "foo\000\000bar") }
    s.exec { server.send_response(sock, :job_created, 'a') }
    s.exec { server.send_response(sock, :work_fail, 'a') }
    s.exec { server.expect_request(sock, :submit_job, "foo\000\000bar") }
    s.exec { server.send_response(sock, :job_created, 'b') }
    s.exec { server.send_response(sock, :work_fail, 'b') }
    s.exec { server.expect_request(sock, :submit_job, "foo\000\000bar") }
    s.exec { server.send_response(sock, :job_created, 'c') }
    s.exec { server.send_response(sock, :work_status, "c\0001\0002") }
    s.exec { server.send_response(sock, :work_fail, 'c') }
    c.wait
    s.wait

    assert_equal(true, failed)
    assert_equal(2, retries)
    assert_equal(1, num)
    assert_equal(2, den)
  end

  def test_failure
    server = FakeJobServer.new(self)
    client, task1, task2, taskset, sock = nil
    res1, res2, fail1, fail2, setres = nil

    s = TestScript.new
    c = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new("localhost:#{server.port}", 'pre') }

    c.exec { task1 = Gearman::Task.new('func1', 'a') }
    c.exec { task1.on_complete {|d| res1 = d } }
    c.exec { task1.on_fail { fail1 = true } }
    c.exec { taskset = Gearman::TaskSet.new(client) }
    c.exec { taskset.add_task(task1) }
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :submit_job, "pre\tfunc1\000\000a") }
    s.exec { server.send_response(sock, :job_created, 'a') }

    c.exec { task2 = Gearman::Task.new('func2', 'b') }
    c.exec { task2.on_complete {|d| res2 = d } }
    c.exec { task2.on_fail { fail2 = true } }
    c.exec { taskset.add_task(task2) }

    s.exec { server.expect_request(sock, :submit_job, "pre\tfunc2\000\000b") }
    s.exec { server.send_response(sock, :job_created, 'b') }

    s.exec { server.send_response(sock, :work_complete, "a\000a1") }
    s.exec { server.send_response(sock, :work_fail, "b") }

    c.exec { setres = taskset.wait }
    c.wait
    s.wait

    assert_equal('a1', res1)
    assert_equal(nil, res2)
    assert_equal(nil, fail1)
    assert_equal(true, fail2)
    assert_equal(false, setres)
  end
end
