#!/usr/bin/env ruby

require 'socket'
require 'thread'

class FakeJobServer
  def initialize(tester,port=nil)
    @tester = tester
    @serv = TCPserver.open(0) if port.nil?
    @serv = TCPserver.open('localhost',port) unless port.nil?
    @port = @serv.addr[1]
  end
  attr_reader :port

  def server_socket
    @serv
  end

  def stop
    @serv.close
  end

  def start
    @serv = TCPserver.open(@port)
  end

  def expect_connection
    sock = @serv.accept
    return sock
  end

  def expect_closed(sock)
    @tester.assert_true(sock.closed?)
  end

  def expect_request(sock, exp_type, exp_data='', size=12)
    head = sock.recv(size)
    magic, type, len = head.unpack('a4NN')
    @tester.assert("\0REQ" == magic || "\000REQ" == magic)
    @tester.assert_equal(Gearman::Util::NUMS[exp_type.to_sym], type)
    data = len > 0 ? sock.recv(len) : ''
    @tester.assert_equal(exp_data, data)
  end

  def expect_any_request(sock)
    head = sock.recv(12)
  end

  def expect_anything_and_close_socket(sock)
    head = sock.recv(12)
    sock.close
  end

  def send_response(sock, type, data='', bogus_size=nil)
    type_num = Gearman::Util::NUMS[type.to_sym] || 0
    response = "\0RES" + [type_num, (bogus_size or data.size)].pack('NN') + data
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
        f = @blocks[0] if not @blocks.empty?
      end
      f.call if f
      @mutex.synchronize do
        @blocks.shift
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
