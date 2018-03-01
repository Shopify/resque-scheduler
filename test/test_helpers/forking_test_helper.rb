module ForkingTestHelper
  def fork_with_marshalled_pipe_and_result
    pipe_read, pipe_write = IO.pipe
    pid = fork do
      pipe_read.close
      result = begin
        [yield, nil]
      rescue StandardError => exc
        [nil, exc]
      end

      pipe_write.syswrite(Marshal.dump(result))
      # exit true the process to get around fork issues on minitest 5
      # see https://github.com/seattlerb/minitest/issues/467
      Process.exit!(true)
    end
    pipe_write.close

    [pid, pipe_read]
  end

  def get_results_from_children(children)
    results = []
    children.each do |pid, pipe|
      wait_for_child_process_to_terminate(pid)

      raise "forked process failed with #{$CHILD_STATUS}" unless $CHILD_STATUS.success?
      result, exc = Marshal.load(pipe.read)
      raise exc if exc
      results << result
    end
    results
  end

  def wait_for_child_process_to_terminate(pid = -1, timeout = 30)
    Timeout.timeout(timeout) do
      Process.wait(pid)
    end
  rescue Timeout::Error
    Process.kill('KILL', pid)
    # collect status so it doesn't stick around as zombie process
    Process.wait(pid)
    flunk 'Child process did not terminate in time.'
  end

end
