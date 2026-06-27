module PgLwwSync
  # Chosen to be stable and unlikely to collide with application advisory locks.
  # Must be a signed 64-bit integer — this value is within that range.
  BACKGROUND_WORKER_LOCK_ID = 7_482_910_492_810

  class Railtie < Rails::Railtie
    rake_tasks do
      load "tasks/pg_lww_sync_tasks.rake"

      %w[db:migrate db:rollback db:schema:load].each do |task_name|
        Rake::Task[task_name].enhance do
          begin
            table_exists = ActiveRecord::Base.connection.select_value(<<~SQL)
              SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'pg_lww_sync' AND table_name = 'pg_lww_changesets'
              );
            SQL

            if ActiveRecord::Base.connected? && table_exists
              puts "PgLwwSync: Executing post-migration realignment..."
              PgLwwSync.initialize_database_functions!
              PgLwwSync.sync_all_tables!
            end
          rescue => e
            puts "PgLwwSync Hook Warning: Alignment operation skipped: #{e.message}"
          end
        end
      end
    end

    initializer "pg_lww_sync.configure_middleware" do |app|
      app.config.middleware.insert_before ActiveRecord::Migration::CheckPending, PgLwwSync::RequestRouter
    end

    initializer "pg_lww_sync.spawn_background_consumer" do
      Rails.application.config.after_initialize do
        next unless defined?(Rails::Server) || defined?(Puma)

        PgLwwSync.assert_valid_configuration!
        PgLwwSync.assert_minimum_postgres_version!

        worker_threads     = 3
        pool_config        = ActiveRecord::Base.connection_db_config
        current_pool_limit = pool_config.configuration_hash[:pool].to_i
        current_pool_limit = 5 if current_pool_limit.zero?

        min_required = 1 + worker_threads + 5
        if current_pool_limit < min_required
          raise "[PgLwwSync Error] Connection pool too small (#{current_pool_limit}). " \
                "Set pool: >= #{min_required} in database.yml."
        end

        # consumer is captured in closures below so signal handlers and Puma
        # hooks can call stop! on the same instance the supervisor is running.
        consumer = nil
        consumer_mutex = Mutex.new

        # ----------------------------------------------------------------
        # SIGTERM — standard process shutdown (systemd, Heroku, k8s, etc.)
        # ----------------------------------------------------------------
        # Puma installs its own SIGTERM handler; we chain onto it rather than
        # replacing it so Puma's own graceful shutdown still runs correctly.
        previous_sigterm = Signal.trap("SIGTERM") do
          # Signal handlers run on the main thread; hand off to a separate thread
          # so we can use mutexes and logging safely without deadlock risk.
          Thread.new do
            Rails.logger.info "[PgLwwSync] SIGTERM received — initiating graceful shutdown."
            consumer_mutex.synchronize { consumer&.stop! }
          end
          # Chain to Puma's previous handler (may be a Proc, "DEFAULT", or "IGNORE")
          case previous_sigterm
          when Proc    then previous_sigterm.call
          when "DEFAULT" then raise SignalException, "SIGTERM"
          end
        end

        # ----------------------------------------------------------------
        # Puma phased restart (USR1) — used by Puma clustered mode to roll
        # workers without dropping the master process. Each old worker gets
        # a `before_fork` callback followed by a graceful shutdown window.
        # ----------------------------------------------------------------
        if defined?(Puma::Server) || defined?(Puma::Cluster)
          # `on_worker_shutdown` fires in each Puma worker process just before
          # it exits during a phased restart — the right place to drain cleanly.
          Puma.respond_to?(:cli_config) && Puma.cli_config&.options&.dig(:before_worker_shutdown)&.tap do |hooks|
            hooks << proc do
              Rails.logger.info "[PgLwwSync] Puma worker shutdown hook — stopping consumer."
              consumer_mutex.synchronize { consumer&.stop! }
            end
          end

          # Fallback: hook into the Puma runner directly if cli_config isn't available
          # (e.g. when Puma is loaded via config/puma.rb rather than the CLI).
          if defined?(Puma::Runner)
            Puma::Runner.prepend(Module.new do
              def stop_blocked
                Rails.logger.info "[PgLwwSync] Puma runner stopping — stopping consumer."
                consumer_mutex.synchronize { consumer&.stop! }
                super
              end
            end)
          end
        end

        # ----------------------------------------------------------------
        # Supervisor thread — acquires the cluster advisory lock and runs
        # the consumer. Restarts automatically if the consumer crashes.
        # ----------------------------------------------------------------
        Thread.new do
          loop do
            break if consumer_mutex.synchronize { consumer&.instance_variable_get(:@shutdown) }

            conn = nil
            begin
              conn = ActiveRecord::Base.connection_pool.checkout

              lock_acquired = conn.select_value(
                "SELECT pg_try_advisory_lock(#{BACKGROUND_WORKER_LOCK_ID});"
              )

              if lock_acquired == 't' || lock_acquired == true
                Rails.logger.info "[PgLwwSync] Leader lock acquired on PID #{Process.pid}. Starting consumer."

                PgLwwSync.initialize_database_functions!

                consumer_mutex.synchronize do
                  consumer = PgLwwSync::Consumer.new(
                    batch_size:     250,
                    sleep_interval: 0.05,
                    pool_size:      worker_threads
                  )
                end

                # start! blocks until stop! is called or an unrecoverable error occurs.
                consumer.start!

                # If we get here, stop! was called — exit the supervisor loop cleanly.
                break
              else
                ActiveRecord::Base.connection_pool.checkin(conn)
                conn = nil
                sleep 30
              end
            rescue => e
              Rails.logger.error "[PgLwwSync Supervisor] Exception: #{e.message}. Restarting in 10s."
              begin
                ActiveRecord::Base.connection_pool.checkin(conn) if conn
              rescue
                nil
              end
              conn = nil
              sleep 10
            end
          end

          Rails.logger.info "[PgLwwSync] Supervisor thread exiting."
        end
      end
    end
  end
end
