module Resque
  module Scheduler
    module Lua
      extend self

      LUA_SCRIPTS_PATH = File.join(File.dirname(__FILE__), 'lua')

      def zpop(key, min, max, offset, count)
        evalsha(:zpop, [key], [min, max, offset, count])
      end

      private

      def zpop_sha(refresh = false)
        @zpop_sha = nil if refresh
        @zpop_sha ||= Resque.redis.script(:load, load_lua("zpop"))
      end

      def evalsha(script, keys, argv, refresh: false)
        sha_method_name = "#{script}_sha"
        Resque.redis.evalsha(send(sha_method_name, refresh), keys: keys, argv: argv)
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
    end
  end
end
