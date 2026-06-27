require 'json'

module PgLwwSync
  class Consumer
    MAX_HORIZON_AGE_SECONDS  = 60
    HORIZON_CACHE_TTL        = 10
    PRUNE_EVERY_N_ITERATIONS = 100
    PRUNE_RETAIN_DAYS        = 7
    QUEUE_PUSH_TIMEOUT       = 5
    # How long stop! waits for in-flight worker jobs to finish before giving up.
    GRACEFUL_SHUTDOWN_TIMEOUT = 30

    def initialize(batch_size: 250, sleep_interval: 5, pool_size: 3)
      @batch_size      = batch_size
      @sleep_interval  = sleep_interval
      @pool_size       = pool_size
      @iteration_count = 0

      # Monotonic clock reference point for horizon TTL — immune to wall-clock
      # adjustments from DST changes or NTP corrections.
      @last_horizon_check_mono = nil
      @cached_horizon_blocked  = false

      @local_db_mutex = Mutex.new
      @work_queue     = SizedQueue.new(@pool_size * 4)

      # Written by stop! (from a signal handler or Puma hook); read by the main
      # loop and workers. Declared before spawn_worker so threads see it immediately.
      @shutdown = false

      # Worker registry — maintained by the main loop so dead workers can be
      # detected and respawned without restarting the entire consumer.
      @workers      = []
      @workers_mutex = Mutex.new
    end

    def start!
      Rails.logger.info "[PgLwwSync Consumer] Spinning up with #{@pool_size} worker thread(s)."

      @pool_size.times { spawn_and_register_worker }

      until @shutdown
        begin
          # Periodically check for dead workers and respawn them so the pool
          # stays at @pool_size regardless of unhandled exceptions or JVM GC issues.
          reap_and_respawn_workers

          process_next_replication_batch!
          @iteration_count += 1
          prune_delivered_changesets! if (@iteration_count % PRUNE_EVERY_N_ITERATIONS).zero?
        rescue => e
          Rails.logger.error "[PgLwwSync Consumer] Main loop error: #{e.class}: #{e.message}"
          sleep 2
          next
        end

        sleep @sleep_interval
      end

      drain_and_shutdown_workers
    end

    # Called by the signal handler or Puma phased-restart hook.
    # Safe to call from any thread.
    def stop!
      Rails.logger.info "[PgLwwSync Consumer] Shutdown requested — draining in-flight work."
      @shutdown = true
    end

    def process_next_replication_batch!
      rows = fetch_pending_outbox_changesets
      return if rows.empty?

      rows.group_by { |r| r['transaction_id'] }.each do |transaction_id, tx_rows|
        deadline = monotonic_now + QUEUE_PUSH_TIMEOUT
        loop do
          begin
            @work_queue.push([transaction_id, tx_rows], true)
            break
          rescue ThreadError
            if monotonic_now >= deadline
              Rails.logger.warn "[PgLwwSync Consumer] Work queue full after #{QUEUE_PUSH_TIMEOUT}s — workers may be hung. Skipping batch."
              return
            end
            sleep 0.1
          end
        end
      end
    end

    private

    # -----------------------------------------------------------------------
    # Worker lifecycle
    # -----------------------------------------------------------------------

    def spawn_and_register_worker
      t = Thread.new do
        until @shutdown
          transaction_id, tx_rows = @work_queue.pop
          # pop returns nil when the queue is closed (shutdown path)
          break if transaction_id.nil?

          begin
            process_transaction_bucket(transaction_id, tx_rows)
          rescue => e
            Rails.logger.error "[PgLwwSync Worker] Unhandled error for tx #{transaction_id}: #{e.class}: #{e.message}"
          ensure
            ActiveRecord::Base.connection_pool.release_connection
          end
        end
      end

      @workers_mutex.synchronize { @workers << t }
      t
    end

    # Called on every main-loop iteration. Removes dead threads and spawns
    # replacements so the pool stays at exactly @pool_size live workers.
    def reap_and_respawn_workers
      @workers_mutex.synchronize do
        dead = @workers.reject(&:alive?)
        dead.each do |t|
          Rails.logger.warn "[PgLwwSync Consumer] Worker thread #{t.object_id} died — respawning."
          @workers.delete(t)
        end
      end

      # Spawn outside the mutex — spawn_and_register_worker re-acquires it internally
      live_count = @workers_mutex.synchronize { @workers.count(&:alive?) }
      (@pool_size - live_count).times { spawn_and_register_worker } if live_count < @pool_size
    end

    # Signals all workers to stop, waits up to GRACEFUL_SHUTDOWN_TIMEOUT for
    # in-flight jobs to complete, then forcibly kills any stragglers.
    def drain_and_shutdown_workers
      Rails.logger.info "[PgLwwSync Consumer] Waiting up to #{GRACEFUL_SHUTDOWN_TIMEOUT}s for workers to drain."

      # Closing the queue unblocks any worker blocked on pop, which then sees
      # a nil item and exits its loop cleanly.
      @work_queue.close if @work_queue.respond_to?(:close)  # SizedQueue#close added in Ruby 2.7

      deadline = monotonic_now + GRACEFUL_SHUTDOWN_TIMEOUT
      @workers_mutex.synchronize { @workers.dup }.each do |t|
        remaining = deadline - monotonic_now
        if remaining > 0
          t.join(remaining)
        end
        if t.alive?
          Rails.logger.warn "[PgLwwSync Consumer] Worker #{t.object_id} did not finish in time — killing."
          t.kill
        end
      end

      Rails.logger.info "[PgLwwSync Consumer] Shutdown complete."
    end

    # -----------------------------------------------------------------------
    # Timing helpers — all use CLOCK_MONOTONIC, never wall-clock Time
    # -----------------------------------------------------------------------

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # -----------------------------------------------------------------------
    # Fetching and processing
    # -----------------------------------------------------------------------

    def fetch_pending_outbox_changesets
      now_mono = monotonic_now

      if @last_horizon_check_mono.nil? || (now_mono - @last_horizon_check_mono) > HORIZON_CACHE_TTL
        @last_horizon_check_mono = now_mono

        begin
          max_active_age = ActiveRecord::Base.connection.select_value(<<~SQL).to_f
            SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - query_start)), 0) AS age
            FROM pg_stat_activity
            WHERE state != 'idle' AND backend_type = 'client backend'
            ORDER BY age DESC LIMIT 1;
          SQL
          @cached_horizon_blocked = (max_active_age > MAX_HORIZON_AGE_SECONDS)
        rescue => e
          Rails.logger.warn "[PgLwwSync Consumer] Failed to query pg_stat_activity: #{e.message}. Defaulting to unblocked."
          @cached_horizon_blocked = false
        end
      end

      sql = if @cached_horizon_blocked
        <<~SQL
          SELECT * FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
          WHERE status = 'pending'
            AND transaction_id IN (
              SELECT transaction_id FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
              WHERE status = 'pending'
              GROUP BY transaction_id
              HAVING MAX(committed_at) < (clock_timestamp() - interval '5 seconds')
            )
          ORDER BY committed_at ASC, id ASC
          LIMIT #{@batch_size};
        SQL
      else
        xmin_horizon = ActiveRecord::Base.connection.select_value(
          "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        ).to_i
        <<~SQL
          SELECT * FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
          WHERE status = 'pending'
            AND transaction_id < #{xmin_horizon}
          ORDER BY committed_at ASC, id ASC
          LIMIT #{@batch_size};
        SQL
      end

      ActiveRecord::Base.connection.select_all(sql).to_a
    end

    def process_transaction_bucket(transaction_id, rows)
      PgLwwSync.remote_nodes.each do |node|
        target_node_id = node[:node_id]

        next if rows.any? { |r| r['origin_node_id'] == target_node_id }
        next if rows.all? { |r| parse_processed_nodes(r['processed_nodes'])[target_node_id] == 'success' }

        begin
          ReplicationClient.connect_to(node) do |remote_conn|
            remote_conn.transaction do
              remote_conn.execute("SET LOCAL pg_lww_sync.applying_replication = 'on';")
              rows.each { |row| remote_conn.execute(format_lww_sql_call(row, target_node_id)) }
            end
          end

          @local_db_mutex.synchronize { update_local_node_status(transaction_id, target_node_id, 'success') }
        rescue => e
          handle_replication_failure(rows, target_node_id, e)
          @local_db_mutex.synchronize { quarantine_poison_pill_transaction(transaction_id, target_node_id) }
          next
        end
      end
    end

    def quarantine_poison_pill_transaction(transaction_id, target_node_id)
      conn = ActiveRecord::Base.connection
      conn.execute(<<~SQL)
        UPDATE #{PgLwwSync::TARGET_CHANGESET_TABLE}
        SET status = 'failed',
            processed_nodes = processed_nodes || jsonb_build_object(#{conn.quote(target_node_id)}, 'failed')
        WHERE transaction_id = #{transaction_id.to_i} AND status = 'pending';
      SQL
    end

    def update_local_node_status(transaction_id, target_node_id, state)
      conn = ActiveRecord::Base.connection
      conn.execute(<<~SQL)
        UPDATE #{PgLwwSync::TARGET_CHANGESET_TABLE}
        SET processed_nodes = processed_nodes || jsonb_build_object(#{conn.quote(target_node_id)}, #{conn.quote(state)})
        WHERE transaction_id = #{transaction_id.to_i}
          AND status != 'failed';
      SQL
    end

    def handle_replication_failure(tx_rows, node_id, error_exception)
      sample_row   = tx_rows.first || {}
      context_data = {
        target_node_id:    node_id,
        failed_schema:     sample_row['table_schema'],
        failed_table:      sample_row['table_name'],
        primary_record_id: sample_row['record_id'],
        action_type:       sample_row['action_type'],
        transaction_id:    sample_row['transaction_id']
      }

      Rails.logger.error <<~MSG
        === [PgLwwSync Poison Pill Quarantined] ===
        Target Peer Node: #{node_id}
        Replication Path: #{context_data[:failed_schema]}.#{context_data[:failed_table]} (ID: #{context_data[:primary_record_id]})
        Engine Action:    #{context_data[:action_type]}
        Exception Class:  #{error_exception.class.name}
        Error Details:    #{error_exception.message}
        STATUS:           Transaction #{context_data[:transaction_id]} marked as 'failed' for #{node_id}.
        ===========================================
      MSG

      if PgLwwSync.on_replication_failure.respond_to?(:call)
        begin
          PgLwwSync.on_replication_failure.call(error_exception, context_data)
        rescue => callback_error
          Rails.logger.error "[PgLwwSync Callback Crash] on_failure block raised: #{callback_error.message}"
        end
      end
    end

    def format_lww_sql_call(row, _target_node_id)
      conn = ActiveRecord::Base.connection
      # record_id is stored as JSONB, pass directly
      record_id = row['record_id'].is_a?(String) ? row['record_id'] : row['record_id'].to_json
      <<~SQL
        SELECT pg_lww_sync.apply_lww_change(
          #{conn.quote(row['table_schema'])},
          #{conn.quote(row['table_name'])},
          #{conn.quote(record_id)}::jsonb,
          #{conn.quote(row['action_type'])},
          #{conn.quote(row['changed_fields'])}::jsonb,
          #{conn.quote(row['column_timestamps'])}::jsonb,
          #{conn.quote(row['origin_node_id'])}
        );
      SQL
    end

    def prune_delivered_changesets!
      cutoff = PRUNE_RETAIN_DAYS.days.ago
      result = ActiveRecord::Base.connection.execute(<<~SQL)
        DELETE FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
        WHERE status = 'success'
          AND committed_at < #{ActiveRecord::Base.connection.quote(cutoff.iso8601)};
      SQL
      pruned = result.cmd_tuples
      Rails.logger.info "[PgLwwSync Consumer] Pruned #{pruned} delivered changesets older than #{PRUNE_RETAIN_DAYS} days." if pruned > 0

      # Also prune column_timings table to keep metadata growth in check
      prune_column_timings!
    rescue => e
      Rails.logger.warn "[PgLwwSync Consumer] Prune failed: #{e.message}"
    end

    def prune_column_timings!
      conn = ActiveRecord::Base.connection
      begin
        interval = "#{PRUNE_RETAIN_DAYS} days"
        # Call the database helper function; it returns the number of rows removed.
        result = conn.select_value("SELECT pg_lww_sync.prune_column_timings(#{conn.quote(interval)}::interval);")
        pruned = result.to_i
        Rails.logger.info "[PgLwwSync Consumer] Pruned #{pruned} column_timings entries older than #{PRUNE_RETAIN_DAYS} days." if pruned > 0
      rescue => e
        Rails.logger.warn "[PgLwwSync Consumer] Column timings prune failed: #{e.message}"
      end
    end

    def parse_processed_nodes(field)
      return {} if field.nil?
      field.is_a?(Hash) ? field : JSON.parse(field)
    end
  end
end
