require 'spec_helper'
require 'socket'
require 'rspec'
require 'rspec/mocks'
require 'gearman'

describe Gearman::Task do
  before(:each) do

  end

  context :creation do
    it "throws an exception when passed bogus opts" do
      expect {
         Gearman::Task.new("bogus_task", "data", {:bogus => 'bogon'})
      }.to raise_error
    end

    it "should generate a task from two arguments" do
      task = Gearman::Task.new("queue", "data")
      task.should_not be nil
    end

    it "should generate a task from three arguments" do
      task = Gearman::Task.new("queue", "data", {:background => true})
      task.should_not be nil
    end

    it "generates a uniq value based on the data and the function" do
      hash_data = 'bc2ca93d86a28cb72fedf36326d1da0cc3d4ed6a'
      task_one = Gearman::Task.new("unique_id", "abcdef")
      expect(task_one.get_uniq_hash).to eq(hash_data)

      task_two = Gearman::Task.new("unique_id", "foobar")
      expect(task_two.get_uniq_hash).to_not eq(hash_data)
    end

    it "honors a uniq value set for the task" do
      task = Gearman::Task.new("unique_id", "abcdef", {:uniq => 'totally_awesome'})
      expect(task.get_uniq_hash).to eq ('91aa6f67b66e394412d9be5f5699c843c726aad8')
    end
  end

  context :get_submit_packet do
    it "represents an EPOCH task properly" do
      task = Gearman::Task.new("function", "data")
      scheduled_for = Time.now
      task.schedule(scheduled_for)

      task_str = task.get_submit_packet
      expect(task_str).to eq "\x00REQ\x00\x00\x00$\x00\x00\x00Afunction\x007dda232f7c04c9d59c0cc43e1c30dea72362e265\x00#{scheduled_for.to_i}\x00data"
    end

    it "represents a background job properly" do
      task = Gearman::BackgroundTask.new("background", "bgdata")
      byte_string = "\x00REQ\x00\x00\x00\x12\x00\x00\x00:background\x00ed75b3f27f59d8e1ed51eedd3b5f98de3141ad49\x00bgdata"
      expect(task.get_submit_packet).to eq byte_string
    end

    it "represents a high priority background job properly" do
      task = Gearman::BackgroundTask.new("background", "highbgdata", {:priority => :high})
      byte_string = "\x00REQ\x00\x00\x00 \x00\x00\x00>background\x003e304e4c11c31c904fbfae7b4f9840ef33006c5e\x00highbgdata"
      expect(task.get_submit_packet).to eq byte_string
    end

    it "represents a low priority background job properly" do
      task = Gearman::BackgroundTask.new("background", "lowbgdata", {:priority => :low})
      byte_string = "\x00REQ\x00\x00\x00\"\x00\x00\x00=background\x0079f123e29effe921e32ce1600b2efc63ab716cad\x00lowbgdata"
      expect(task.get_submit_packet).to eq byte_string
    end

  end


  context :handle_responses do
    before :each do
      @task = Gearman::Task.new("handle_response", "some data")
    end

    it "calls handle_warning properly" do
      warning_data = nil
      @task.on_warning do |msg|
        warning_data = msg
      end

      @task.handle_warning("message")
      expect(warning_data).to eq "message"
    end

    it "calls handle_status properly" do
      numerator = nil
      denominator = nil

      @task.on_status do |n, d|
        numerator = n
        denominator = d
      end

      @task.handle_status(10, 100)
      expect(numerator).to eq 10
      expect(denominator).to eq 100
    end

    it "calls handle_failure correctly" do
      error_message = nil

      @task.on_fail do
        error_message = "oops!"
      end

      @task.handle_failure

      expect(error_message).to eq "oops!"
    end

    it "calls handle_data correctly" do
      data_result = nil

      @task.on_data do |data|
        data_result = data
      end

      @task.handle_data("foonarf")

      expect(data_result).to eq "foonarf"
    end

    it "calls handle_created correctly" do
      job_handle = nil

      @task.on_created do |data|
        job_handle = data
      end

      @task.handle_created("foonarf.local:1234")

      expect(job_handle).to eq "foonarf.local:1234"
    end

    it "calls handle_exception correctly" do
      exception_message = nil

      @task.on_exception do |data|
        exception_message = data
      end

      @task.handle_exception("NetworkError")

      expect(exception_message).to eq "NetworkError"
    end

    it "retries if failure occurs and the failure count is greater than zero" do
      retries_completed = 0
      @task.retry_count = 5
      @task.on_retry do |retries_done|
        retries_completed = retries_done
      end
      @task.handle_failure.should == true
      retries_completed.should == 1
    end

    it "does not retry if the number of retries completed has met the number of retries to execute" do
      @task.retry_count = 3
      retries_completed = 0
      (0..@task.retry_count-1).each do |i|
        retries_completed += 1
        @task.handle_failure.should == true
      end
      @task.handle_failure.should == false
      retries_completed.should == 3
    end
  end
end
