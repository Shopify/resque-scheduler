require "digest"

module Resque
  module Scheduler
    module Lua
      extend self

      def zpop(key, min, max, offset, count)
        ZPOP.call(Resque.redis, [key], [min, max, offset, count])
      end

      def deleq(key, expected_value)
        DELEQ.call(Resque.redis, [key], [expected_value])
      end

      def locked(lock_key, token, timeout)
        LOCKED.call(Resque.redis, [lock_key], [token, timeout])
      end

      private

      LUA_SCRIPTS_PATH = File.join(File.dirname(__FILE__), 'lua')

      Script = Struct.new(:body) do
        def self.from_file(filename)
          new(File.read(LUA_SCRIPTS_PATH + "/#{filename}.lua"))
        end

        def call(conn, keys, argv)
          conn.evalsha(sha1, keys: keys, argv: argv)
        rescue Redis::CommandError => ex
          if ex.message =~ /\ANOSCRIPT/
            conn.eval(body, keys: keys, argv: argv)
          else
            raise
          end
        end

        private

        def sha1
          @sha ||= Digest::SHA1.hexdigest(body)
        end
      end

      ZPOP = Script.from_file("zpop")
      DELEQ = Script.from_file("deleq")
      LOCKED = Script.from_file("locked")
    end
  end
end
