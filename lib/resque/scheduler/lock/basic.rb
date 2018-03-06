# vim:fileencoding=utf-8
require_relative 'base'

module Resque
  module Scheduler
    module Lock
      class Basic < Base
        def acquire!
          Resque.redis.set(key, value, ex: timeout, nx: true)
        end

        def locked?
          if Resque.redis.get(key) == value
            extend_lock!

            return true if Resque.redis.get(key) == value
          end

          false
        end


        def release_if_locked
          locked? && release!
        end
      end
    end
  end
end
