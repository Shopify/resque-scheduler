module Resque
  module Scheduler
    module Lua
      extend self

      LUA_SCRIPTS_PATH = File.join(File.dirname(__FILE__), 'lua')

      def zpop(key, min, max, offset, count)
        evalsha(:zpop, [key], [min, max, offset, count])
      end

      def locked(lock_key, token, timeout)
        evalsha(:locked, [lock_key], [token, timeout])
      end

      private

      def get_sha(script, refresh = false)
        sha_store[script] = nil if refresh
        sha_store[script] ||= Resque.redis.script(:load, load_lua(script))
      end

      def evalsha(script, keys, argv, refresh: false)
        Resque.redis.evalsha(get_sha(script, refresh), keys: keys, argv: argv)
      rescue Redis::CommandError => e
        if e.message =~ /NOSCRIPT/
          refresh = true
          retry
        end
        raise
      end

      def load_lua(filename)
        File.read(LUA_SCRIPTS_PATH + "/#{filename}.lua")
      end

      def sha_store
        @sha_store ||= {}
      end
    end
  end
end
