module PgLwwSync
  class RequestRouter
    # Class-level mutex so pool registration is serialised across all instances
    # and all threads, regardless of how many times Rack instantiates this class.
    POOL_REGISTRATION_MUTEX = Mutex.new

    def initialize(app)
      @app = app
    end

    def call(env)
      blacklisted_node_ids = []
      retries = 0
      max_retries = PgLwwSync.remote_nodes.size + 1
      primary_node_config = nil

      begin
        primary_node_config = PgLwwSync::Consensus.determine_active_primary(blacklist: blacklisted_node_ids)
        ensure_pool_registered!(primary_node_config)

        ActiveRecord::Base.connected_to(
          role:           primary_node_config[:node_id].to_sym,
          prevent_writes: false
        ) do
          @app.call(env)
        end

      rescue PG::ConnectionBad, ActiveRecord::ConnectionNotEstablished, ActiveRecord::StatementInvalid => e
        if primary_node_config && retries < max_retries
          Rails.logger.error "[PgLwwSync Middleware] Node #{primary_node_config[:node_id]} failed: #{e.message}"
          blacklisted_node_ids << primary_node_config[:node_id]
          retries += 1
          PgLwwSync::Consensus.invalidate_fast_path!
          retry
        else
          raise
        end
      end
    end

    private

    def ensure_pool_registered!(node_config)
      role = node_config[:node_id].to_sym

      return if ActiveRecord::Base.connection_handler.retrieve_connection_pool(
        "ActiveRecord::Base", role: role
      )

      # Class-level mutex: protects against concurrent registration from multiple
      # threads or Rack instances simultaneously attempting the same role.
      POOL_REGISTRATION_MUTEX.synchronize do
        return if ActiveRecord::Base.connection_handler.retrieve_connection_pool(
          "ActiveRecord::Base", role: role
        )

        ActiveRecord::Base.connection_handler.establish_connection(
          {
            adapter:  node_config[:adapter]  || "postgresql",
            host:     node_config[:host],
            port:     node_config[:port]     || 5432,
            database: node_config[:database],
            username: node_config[:username],
            password: node_config[:password],
            pool:     node_config[:pool]     || 5
          },
          owner_name: "ActiveRecord::Base",
          role:       role
        )
      end
    end
  end
end
