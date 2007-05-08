#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'socket'
require 'test/unit'
require 'thread'

Thread.abort_on_exception = true

class FakeJobServer
  def initialize(tester)
    @tester = tester
    @serv = TCPserver.open(0)
    @port = @serv.addr[1]
  end
  attr_reader :port

  def expect_connection
    sock = @serv.accept
    return sock
  end

  def expect_closed(sock)
    @tester.assert_true(sock.closed?)
  end

  def expect_request(sock, exp_type, exp_data='')
    head = sock.recv(12)
    magic, type, len = head.unpack('a4NN')
    @tester.assert_equal("\0REQ", magic)
    @tester.assert_equal(Gearman::Util::NUMS[exp_type.to_sym], type)
    data = len > 0 ? sock.recv(len) : ''
    @tester.assert_equal(exp_data, data)
  end

  def send_response(sock, type, data='')
    type_num = Gearman::Util::NUMS[type.to_sym]
    response = "\0RES" + [type_num, data.size].pack('NN') + data
    sock.write(response)
  end
end

class TestScript
  def initialize
    @mutex = Mutex.new
    @cv = ConditionVariable.new
    @blocks = []
  end

  def loop_forever
    loop do
      f = nil
      @mutex.synchronize do
        @cv.wait(@mutex) if @blocks.empty?
        f = @blocks.shift
      end
      f.call if f
      @mutex.synchronize do
        @cv.signal if @blocks.empty?
      end
    end
  end

  def exec(&f)
    @mutex.synchronize do
      @blocks << f
      @cv.signal
    end
  end

  def wait
    @mutex.synchronize do
      @cv.wait(@mutex) if not @blocks.empty?
    end
  end
end

class TestWorker < Test::Unit::TestCase
  def test_worker
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
    s.exec { worker.remove_ability('echo') }
    s.exec { server.expect_request(sock, :cant_do, 'echo') }
    s.wait
  end
end
