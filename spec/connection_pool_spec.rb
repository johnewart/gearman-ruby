
require 'spec_helper'
require 'socket'

describe Gearman::ConnectionPool do
  context "normalizing job servers" do
    before :each do
      @connection_pool = Gearman::ConnectionPool.new
    end

    it "should handle a string for input" do
      connection = Gearman::Connection.new("localhost", 1234)
      connection.should_receive(:is_healthy?).and_return true
      Gearman::Connection.should_receive(:new).with("localhost", 1234).and_return connection
      @connection_pool.add_servers("localhost:1234")
      @connection_pool.get_connection.should be connection
    end

    it "should handle an array of host:port without changing a thing" do
      connection_one = Gearman::Connection.new("localhost", 123)
      connection_one.should_receive(:is_healthy?).and_return true
      connection_two = Gearman::Connection.new("localhost", 456)
      connection_two.should_receive(:is_healthy?).and_return true

      Gearman::Connection.should_receive(:new).with("localhost", 123).and_return connection_one
      Gearman::Connection.should_receive(:new).with("localhost", 456).and_return connection_two

      servers = [ "#{connection_one.to_host_port}", "#{connection_two.to_host_port}" ]
      @connection_pool.add_servers(servers)


      @connection_pool.get_connection.should be connection_two
      @connection_pool.get_connection.should be connection_one

    end

    it "should append the default port to anything in the array that doesn't have a port" do
      in_servers = ["foo.bar.com:123", "narf.quiddle.com"]
      out_servers = ["foo.bar.com:123", "narf.quiddle.com:4730"]

      connection_one = Gearman::Connection.new("foo.bar.com", 123)
      connection_one.should_receive(:is_healthy?).and_return true
      connection_two = Gearman::Connection.new("narf.quiddle.com", 4730)
      connection_two.should_receive(:is_healthy?).and_return true

      Gearman::Connection.should_receive(:new).with("foo.bar.com", 123).and_return connection_one
      Gearman::Connection.should_receive(:new).with("narf.quiddle.com", 4730).and_return connection_two

      @connection_pool.add_servers(in_servers)

      @connection_pool.get_connection.should be connection_two
      @connection_pool.get_connection.should be connection_one
    end
  end
end
