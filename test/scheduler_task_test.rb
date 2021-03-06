# vim:fileencoding=utf-8
require_relative 'test_helper'

context 'Resque::Scheduler' do
  setup do
    Resque::Scheduler.configure do |c|
      c.dynamic = false
      c.poll_sleep_amount = 0.1
    end
    Resque.redis.redis.flushall
    Resque::Scheduler.quiet = true
    Resque::Scheduler.clear_schedule!
    Resque::Scheduler.send(:instance_variable_set, :@scheduled_jobs, {})
    Resque::Scheduler.send(:instance_variable_set, :@shutdown, false)
  end

  test 'shutdown raises Interrupt when sleeping' do
    Thread.current.expects(:raise).with(Interrupt)
    Resque::Scheduler.send(:instance_variable_set, :@th, Thread.current)
    Resque::Scheduler.send(:instance_variable_set, :@sleeping, true)
    Resque::Scheduler.shutdown
  end

  test 'sending TERM to scheduler breaks out of poll_sleep' do
    Resque::Scheduler.expects(:release_master_lock_if_master)

    @pid = Process.pid
    Thread.new do
      sleep(0.05)
      Process.kill(:TERM, @pid)
    end

    assert_raises SystemExit do
      Resque::Scheduler.run
    end

    Resque::Scheduler.unstub(:release_master_lock_if_master)
    Resque::Scheduler.release_master_lock_if_master
  end

  test 'can start successfully' do
    Resque::Scheduler.poll_sleep_amount = nil

    @pid = Process.pid
    Thread.new do
      sleep(0.15)
      Process.kill(:TERM, @pid)
    end

    assert_raises SystemExit do
      Resque::Scheduler.run
    end
  end

  test 'sending TERM to scheduler breaks out when poll_sleep_amount = 0' do
    Resque::Scheduler.poll_sleep_amount = 0
    Resque::Scheduler.expects(:release_master_lock_if_master)

    @pid = Process.pid
    Thread.new do
      sleep(0.05)
      Process.kill(:TERM, @pid)
    end

    assert_raises SystemExit do
      Resque::Scheduler.run
    end

    Resque::Scheduler.unstub(:release_master_lock_if_master)
    Resque::Scheduler.release_master_lock_if_master
  end
end
