#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'
require 'thread'

Thread.abort_on_exception = true

class TestClient < Test::Unit::TestCase
  ##
  # Do a simple test of the functionality of the client code.
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
    s.wait

    # Check that we got the right answers.
    assert_equal(7, res1)
    assert_equal(15, res2)
  end

  ##
  # Test Client#do_task.
  def test_do_task
    server = FakeJobServer.new(self)
    client, sock, res = nil

    s = TestScript.new
    c = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new("localhost:#{server.port}") }

    c.exec { res = client.do_task('add', '5 2').to_i }
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :submit_job, "add\000\0005 2") }
    s.exec { server.send_response(sock, :job_created, 'a') }
    s.exec { server.send_response(sock, :work_complete, "a\0007") }
    c.wait
    s.wait

    assert_equal(7, res)

    c.exec { res = client.do_task('add', '1 2') }
    s.exec { server.expect_request(sock, :submit_job, "add\000\0001 2") }
    s.exec { server.send_response(sock, :job_created, 'a') }
    s.exec { server.send_response(sock, :work_fail, 'a') }
    c.wait
    s.wait

    assert_equal(nil, res)
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

  ##
  # Test that user-supplied uniq values are handled correctly.
  def test_uniq
    server1 = FakeJobServer.new(self)
    server2 = FakeJobServer.new(self)
    client = nil
    sock1, sock2 = nil
    taskset = nil
    task1, task2, task3, task4 = nil
    res1, res2, res3, res4 = nil
    hostport1 = "localhost:#{server1.port}"
    hostport2 = "localhost:#{server2.port}"

    s1 = TestScript.new
    s2 = TestScript.new
    c = TestScript.new

    server1_thread = Thread.new { s1.loop_forever }.run
    server2_thread = Thread.new { s2.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new }
    c.exec { client.job_servers = [hostport1, hostport2] }
    c.exec { taskset = Gearman::TaskSet.new(client) }

    # Submit a task with uniq key 'u' to the first server.
    c.exec { client.test_hostport = hostport1 }
    c.exec { task1 = Gearman::Task.new('func1', 'arg', { :uniq => 'u' }) }
    c.exec { task1.on_complete {|d| res1 = d.to_i } }
    c.exec { taskset.add_task(task1) }

    s1.exec { sock1 = server1.expect_connection }
    s1.wait

    s1.exec { server1.expect_request(
      sock1, :submit_job, "func1\000#{'u'.hash}\000arg") }
    s1.exec { server1.send_response(sock1, :job_created, 'a') }

    # If we submit a second task with the same key, it should get sent to
    # the same server.
    c.exec { client.test_hostport = hostport2 }
    c.exec { task2 = Gearman::Task.new('func1', 'arg2', { :uniq => 'u' }) }
    c.exec { task2.on_complete {|d| res2 = d.to_i } }
    c.exec { taskset.add_task(task2) }

    s1.exec { server1.expect_request(
      sock1, :submit_job, "func1\000#{'u'.hash}\000arg2") }
    s1.exec { server1.send_response(sock1, :job_created, 'a') }

    # When we create a task with key 'a', it should go to the second
    # server.
    c.exec { task3 = Gearman::Task.new('func1', 'arg', { :uniq => 'a' }) }
    c.exec { task3.on_complete {|d| res3 = d.to_i } }
    c.exec { taskset.add_task(task3) }

    s2.exec { sock2 = server2.expect_connection }
    s2.wait

    s2.exec { server2.expect_request(
      sock2, :submit_job, "func1\000#{'a'.hash}\000arg") }
    s2.exec { server2.send_response(sock2, :job_created, 'b') }

    # If we tell the client to use the first server again and create
    # another job with no uniq key, it should go back to the first server.
    c.exec { client.test_hostport = hostport1 }
    c.exec { task4 = Gearman::Task.new('func1', 'arg') }
    c.exec { task4.on_complete {|d| res4 = d.to_i } }
    c.exec { taskset.add_task(task4) }

    s1.exec { server1.expect_request(
      sock1, :submit_job, "func1\000\000arg") }
    s1.exec { server1.send_response(sock1, :job_created, 'c') }

    # Send back responses for all the handles we've handed out and make
    # sure that we got what we expected.
    c.exec { taskset.wait }
    s1.exec { server1.send_response(sock1, :work_complete, "a\0001") }
    s2.exec { server2.send_response(sock2, :work_complete, "b\0002") }
    s1.exec { server1.send_response(sock1, :work_complete, "c\0003") }

    c.wait
    s1.wait
    s2.wait

    assert_equal(1, res1)
    assert_equal(1, res2)
    assert_equal(2, res3)
    assert_equal(3, res4)

    c.wait
    s1.wait
    s2.wait
  end

  ##
  # Test that '-' uniq values work correctly.
  def test_uniq_dash
    server1 = FakeJobServer.new(self)
    server2 = FakeJobServer.new(self)
    client, taskset, sock1, sock2 = nil
    task1, task2, task3 = nil
    res1, res2, res3 = nil
    hostport1 = "localhost:#{server1.port}"
    hostport2 = "localhost:#{server2.port}"

    s1 = TestScript.new
    s2 = TestScript.new
    c = TestScript.new

    server1_thread = Thread.new { s1.loop_forever }.run
    server2_thread = Thread.new { s2.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new }
    c.exec { client.job_servers = [hostport1, hostport2] }
    c.exec { taskset = Gearman::TaskSet.new(client) }

    # The first task uses uniq = '-' with the argument 'arg'.
    c.exec { client.test_hostport = hostport1 }
    c.exec { task1 = Gearman::Task.new('func1', 'arg', { :uniq => '-' }) }
    c.exec { task1.on_complete {|d| res1 = d.to_i } }
    c.exec { taskset.add_task(task1) }

    s1.exec { sock1 = server1.expect_connection }
    s1.wait

    s1.exec { server1.expect_request(
      sock1, :submit_job, "func1\000#{'arg'.hash}\000arg") }
    s1.exec { server1.send_response(sock1, :job_created, 'a') }

    # The second task uses the same arg, so it should be merged with the
    # first by the server (and also be executed on the first server, even
    # though we've changed the client to use the second by default).
    c.exec { client.test_hostport = hostport2 }
    c.exec { task2 = Gearman::Task.new('func1', 'arg', { :uniq => '-' }) }
    c.exec { task2.on_complete {|d| res2 = d.to_i } }
    c.exec { taskset.add_task(task2) }

    s1.exec { server1.expect_request(
      sock1, :submit_job, "func1\000#{'arg'.hash}\000arg") }
    s1.exec { server1.send_response(sock1, :job_created, 'a') }

    # The third task uses 'arg2', so it should not be merged and instead
    # run on the second server.
    c.exec { task3 = Gearman::Task.new('func1', 'arg2', { :uniq => '-' }) }
    c.exec { task3.on_complete {|d| res3 = d.to_i } }
    c.exec { taskset.add_task(task3) }

    s2.exec { sock2 = server2.expect_connection }
    s2.wait

    s2.exec { server2.expect_request(
      sock2, :submit_job, "func1\000#{'arg2'.hash}\000arg2") }
    s2.exec { server2.send_response(sock2, :job_created, 'b') }

    # Send back results for the two handles that we've handed out.
    c.exec { taskset.wait }
    s1.exec { server1.send_response(sock1, :work_complete, "a\0001") }
    s2.exec { server2.send_response(sock2, :work_complete, "b\0002") }

    c.wait
    s1.wait
    s2.wait

    assert_equal(1, res1)
    assert_equal(1, res2)
    assert_equal(2, res3)
  end

  ##
  # Test that NUL bytes in returned data are preserved.
  def test_nuls_in_data
    server = FakeJobServer.new(self)
    client, sock, res = nil

    s = TestScript.new
    c = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new("localhost:#{server.port}") }

    c.exec { res = client.do_task('foo', nil) }
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :submit_job, "foo\000\000") }
    s.exec { server.send_response(sock, :job_created, 'a') }
    s.exec { server.send_response(sock, :work_complete, "a\0001\0002\0003") }
    c.wait
    s.wait

    assert_equal("1\0002\0003", res)
  end

  ##
  # Test that clients time out when the server sends a partial packet and
  # then hangs.
  def test_read_timeouts
    server = FakeJobServer.new(self)
    client, sock, task, taskset, res = nil

    s = TestScript.new
    c = TestScript.new

    server_thread = Thread.new { s.loop_forever }.run
    client_thread = Thread.new { c.loop_forever }.run

    c.exec { client = Gearman::Client.new("localhost:#{server.port}") }

    # First, create a new task.  The server claims to be sending back a
    # packet with 1 byte of data, but actually sends an empty packet.  The
    # client should time out after 0.1 sec.
    c.exec { taskset = Gearman::TaskSet.new(client) }
    c.exec { task = Gearman::Task.new('foo', 'bar') }
    c.exec { client.task_create_timeout_sec = 0.1 }
    c.exec { res = taskset.add_task(task) }
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :submit_job, "foo\000\000bar") }
    s.exec { server.send_response(sock, :job_created, '', 1) }
    c.wait
    s.wait

    assert_equal(false, res)

    # Now create a task, but only return a partial packet for
    # work_complete.  The client should again time out after 0.1 sec.
    c.exec { res = taskset.add_task(task) }
    s.exec { sock = server.expect_connection }
    s.wait

    s.exec { server.expect_request(sock, :submit_job, "foo\000\000bar") }
    s.exec { server.send_response(sock, :job_created, 'a') }
    c.exec { res = taskset.wait(0.1) }
    s.exec { server.send_response(sock, :work_complete, "a\000", 3) }
    c.wait
    s.wait

    assert_equal(false, res)
  end
end
