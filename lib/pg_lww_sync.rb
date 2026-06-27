require 'yaml'
require 'erb'
require_relative "pg_lww_sync/version"
require_relative "pg_lww_sync/railtie" if defined?(Rails)

module PgLwwSync
  SYSTEM_SCHEMAS = %w[pg_catalog information_schema pg_lww_sync pg_toast].freeze
  SYSTEM_TABLES  = %w[schema_migrations ar_internal_metadata].freeze
  TARGET_CHANGESET_TABLE = "pg_lww_sync.pg_lww_changesets"

  # node_id must be safe to embed in SQL identifiers / JSON keys
  NODE_ID_FORMAT = /\A[a-z0-9_]{1,64}\z/i

  class << self
    attr_reader :on_replication_failure

    def config
      @config ||= load_config_file!
    end

    def node_id
      config[:node_id]
    end

    def remote_nodes
      config[:remote_nodes] || []
    end

    def on_failure(&block)
      @on_replication_failure = block
    end

    def assert_valid_configuration!
      if node_id.blank?
        raise <<~ERROR

          [PgLwwSync] CONFIGURATION ERROR:
          'node_id' is missing or unassigned for the '#{Rails.env}' environment profile.
          Verify your config/pg_lww_sync.yml mapping details.
        ERROR
      end

      unless node_id.match?(NODE_ID_FORMAT)
        raise <<~ERROR

          [PgLwwSync] CONFIGURATION ERROR:
          'node_id' value #{node_id.inspect} is invalid.
          Only alphanumeric characters and underscores are allowed (max 64 chars).
        ERROR
      end

      remote_nodes.each do |node|
        unless node[:node_id].to_s.match?(NODE_ID_FORMAT)
          raise "[PgLwwSync] CONFIGURATION ERROR: remote node_id #{node[:node_id].inspect} is invalid."
        end
      end
    end

    def assert_minimum_postgres_version!
      ActiveRecord::Base.connection_pool.with_connection do |conn|
        if conn.postgresql_version < 130000
          raise "[PgLwwSync Fatal Error] Incompatible Engine! PostgreSQL 13.0+ required."
        end
      end
    end

    # Attaches (or reattaches) the sync trigger to every non-system table.
    # Each table is handled in its own execute call so we never hold DDL locks
    # on multiple tables simultaneously — critical in production with many tables.
    def sync_all_tables!
      assert_valid_configuration!

      conn = ActiveRecord::Base.connection

      # Ensure the trigger function exists before we attach it anywhere
      conn.execute("SELECT pg_lww_sync.log_lww_sync_init();")

      system_schemas_list = SYSTEM_SCHEMAS.map { |s| conn.quote(s) }.join(", ")
      system_tables_list  = SYSTEM_TABLES.map  { |t| conn.quote(t) }.join(", ")

      tables = conn.select_rows(<<~SQL)
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN (#{system_schemas_list})
          AND table_name   NOT IN (#{system_tables_list})
        ORDER BY table_schema, table_name;
      SQL

      tables.each do |schema, table|
        # One execute per table = one lock taken and released per table, not all at once
        conn.execute(<<~SQL)
          DROP TRIGGER IF EXISTS c_pg_lww_sync_tg ON #{conn.quote_table_name("#{schema}.#{table}")};
          CREATE TRIGGER c_pg_lww_sync_tg
          BEFORE INSERT OR UPDATE OR DELETE ON #{conn.quote_table_name("#{schema}.#{table}")}
          FOR EACH ROW EXECUTE PROCEDURE pg_lww_sync.log_lww_sync_changeset();
        SQL
      end
    end

    def initialize_database_functions!
      assert_valid_configuration!

      quoted_node_id = ActiveRecord::Base.connection.quote(PgLwwSync.node_id)

      initial_nodes_json = PgLwwSync.remote_nodes.each_with_object({}) do |node, hash|
        hash[node[:node_id]] = "pending"
      end.to_json
      quoted_initial_nodes_json = ActiveRecord::Base.connection.quote(initial_nodes_json)

      trigger_sql = <<~SQL
        CREATE OR REPLACE FUNCTION pg_lww_sync.log_lww_sync_init() RETURNS void AS $$
        BEGIN
          CREATE OR REPLACE FUNCTION pg_lww_sync.local_node_id()
          RETURNS CHARACTER VARYING AS $node$
          BEGIN
            RETURN #{quoted_node_id}::character varying;
          END;
          $node$ LANGUAGE plpgsql IMMUTABLE;

          CREATE OR REPLACE FUNCTION pg_lww_sync.log_lww_sync_changeset()
          RETURNS TRIGGER AS $tg$
          DECLARE
            v_column_timestamps JSONB := '{}'::jsonb;
            v_now TIMESTAMP WITH TIME ZONE := clock_timestamp();
            v_col TEXT;
            v_old_val TEXT;
            v_new_val TEXT;
            v_record_id TEXT;
            v_tx_id BIGINT := txid_current();
          BEGIN
            -- Skip when this session is applying replicated changes from a peer node,
            -- preventing the trigger from generating a new outbox entry (and thus
            -- preventing an infinite replication loop back to the origin).
            IF pg_is_in_recovery()
               OR current_setting('session_replication_role', true) = 'replica'
               OR current_setting('pg_lww_sync.applying_replication', true) = 'on'
            THEN
              IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
            END IF;

            v_record_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.id::text ELSE NEW.id::text END;

            IF TG_OP = 'INSERT' THEN
              -- Use pg_attribute for catalog access — much faster than information_schema
              FOR v_col IN
                SELECT attname FROM pg_attribute
                WHERE attrelid = (quote_ident(TG_TABLE_SCHEMA) || '.' || quote_ident(TG_TABLE_NAME))::regclass
                  AND attnum > 0 AND NOT attisdropped AND attname != 'id'
                ORDER BY attnum
              LOOP
                v_column_timestamps := v_column_timestamps || jsonb_build_object(v_col, v_now);
              END LOOP;

            ELSIF TG_OP = 'UPDATE' THEN
              FOR v_col IN
                SELECT attname FROM pg_attribute
                WHERE attrelid = (quote_ident(TG_TABLE_SCHEMA) || '.' || quote_ident(TG_TABLE_NAME))::regclass
                  AND attnum > 0 AND NOT attisdropped AND attname != 'id'
                ORDER BY attnum
              LOOP
                EXECUTE format('SELECT ($1).%I::text', v_col) INTO v_old_val USING OLD;
                EXECUTE format('SELECT ($1).%I::text', v_col) INTO v_new_val USING NEW;
                IF (v_old_val IS DISTINCT FROM v_new_val) THEN
                  v_column_timestamps := v_column_timestamps || jsonb_build_object(v_col, v_now);
                END IF;
              END LOOP;
              IF v_column_timestamps = '{}'::jsonb THEN RETURN NEW; END IF;

            ELSIF TG_OP = 'DELETE' THEN
              v_column_timestamps := jsonb_build_object('__deleted__', v_now);
            END IF;

            INSERT INTO #{TARGET_CHANGESET_TABLE} (
              table_schema, table_name, record_id, action_type,
              changed_fields, column_timestamps, transaction_id,
              origin_node_id, committed_at, processed_nodes
            ) VALUES (
              TG_TABLE_SCHEMA, TG_TABLE_NAME, v_record_id, TG_OP,
              CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE row_to_json(NEW) END,
              v_column_timestamps, v_tx_id, pg_lww_sync.local_node_id(),
              v_now, #{quoted_initial_nodes_json}::jsonb
            );

            IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
          END;
          $tg$ LANGUAGE plpgsql;
        END;
        $$ LANGUAGE plpgsql;
      SQL

      lww_engine_sql = <<~SQL
        CREATE OR REPLACE FUNCTION pg_lww_sync.apply_lww_change(
          p_table_schema TEXT,
          p_table_name TEXT,
          p_record_id TEXT,
          p_action_type TEXT,
          p_changed_fields JSON,
          p_column_timestamps JSONB,
          p_origin_node_id TEXT
        ) RETURNS VOID AS $body$
        DECLARE
          v_col TEXT;
          v_incoming_time TIMESTAMP WITH TIME ZONE;
          v_local_time TIMESTAMP WITH TIME ZONE;
          v_update_sql TEXT := '';
          v_insert_cols TEXT := 'id';
          v_insert_vals TEXT := format('%L', p_record_id);
          v_exists BOOLEAN;
          v_local_deleted_at TIMESTAMP WITH TIME ZONE;
          v_incoming_deleted_at TIMESTAMP WITH TIME ZONE;
          v_temp_table_name TEXT;
        BEGIN
          -- ----------------------------------------------------------------
          -- DELETE path: LWW comparison against the most recent local write
          -- ----------------------------------------------------------------
          IF p_action_type = 'DELETE' OR p_column_timestamps ? '__deleted__' THEN
            v_incoming_deleted_at := (p_column_timestamps->>'__deleted__')::TIMESTAMP WITH TIME ZONE;

            SELECT MAX(last_mutated_at) INTO v_local_time
            FROM pg_lww_sync.column_timings
            WHERE table_schema = p_table_schema AND table_name = p_table_name AND record_id = p_record_id;

            IF v_local_time IS NULL
               OR v_incoming_deleted_at > v_local_time
               OR (v_incoming_deleted_at = v_local_time AND p_origin_node_id > pg_lww_sync.local_node_id())
            THEN
              EXECUTE format('DELETE FROM %I.%I WHERE id = %L', p_table_schema, p_table_name, p_record_id);

              INSERT INTO pg_lww_sync.column_timings (table_schema, table_name, record_id, column_name, last_mutated_at)
              VALUES (p_table_schema, p_table_name, p_record_id, '__deleted__', v_incoming_deleted_at)
              ON CONFLICT (table_schema, table_name, record_id, column_name)
              DO UPDATE SET last_mutated_at = EXCLUDED.last_mutated_at
              WHERE EXCLUDED.last_mutated_at > pg_lww_sync.column_timings.last_mutated_at;
            END IF;

            RETURN;
          END IF;

          -- ----------------------------------------------------------------
          -- Temp table: named per schema+table so multiple calls in the same
          -- session for different tables never collide. IF NOT EXISTS is then
          -- safe because same-named = same shape. No DROP needed, no risk of
          -- aborting the transaction on a DROP error.
          -- ----------------------------------------------------------------
          v_temp_table_name := 't_lww_' || p_table_schema || '_' || p_table_name;
          EXECUTE format(
            'CREATE TEMP TABLE IF NOT EXISTS %I (LIKE %I.%I INCLUDING ALL) ON COMMIT DROP',
            v_temp_table_name, p_table_schema, p_table_name
          );
          EXECUTE format('TRUNCATE TABLE %I', v_temp_table_name);
          EXECUTE format(
            'INSERT INTO %I SELECT * FROM json_populate_record(NULL::%I.%I, $1)',
            v_temp_table_name, p_table_schema, p_table_name
          ) USING p_changed_fields;

          -- ----------------------------------------------------------------
          -- Resurrection guard: if record was previously deleted, only apply
          -- incoming write if it is strictly newer than the deletion timestamp.
          -- ----------------------------------------------------------------
          SELECT last_mutated_at INTO v_local_deleted_at
          FROM pg_lww_sync.column_timings
          WHERE table_schema = p_table_schema AND table_name = p_table_name
            AND record_id = p_record_id AND column_name = '__deleted__';

          EXECUTE format(
            'SELECT EXISTS(SELECT 1 FROM %I.%I WHERE id = %L)',
            p_table_schema, p_table_name, p_record_id
          ) INTO v_exists;

          IF NOT v_exists THEN
            -- INSERT path
            FOR v_col IN
              SELECT attname FROM pg_attribute
              WHERE attrelid = (quote_ident(p_table_schema) || '.' || quote_ident(p_table_name))::regclass
                AND attnum > 0 AND NOT attisdropped AND attname != 'id'
              ORDER BY attnum
            LOOP
              v_incoming_time := (p_column_timestamps->>v_col)::TIMESTAMP WITH TIME ZONE;
              IF v_incoming_time IS NOT NULL THEN
                CONTINUE WHEN v_local_deleted_at IS NOT NULL AND v_incoming_time <= v_local_deleted_at;

                v_insert_cols := v_insert_cols || format(', %I', v_col);
                v_insert_vals := v_insert_vals || format(', (SELECT %I FROM %I)', v_col, v_temp_table_name);

                INSERT INTO pg_lww_sync.column_timings (table_schema, table_name, record_id, column_name, last_mutated_at)
                VALUES (p_table_schema, p_table_name, p_record_id, v_col, v_incoming_time)
                ON CONFLICT (table_schema, table_name, record_id, column_name)
                DO UPDATE SET last_mutated_at = EXCLUDED.last_mutated_at
                WHERE EXCLUDED.last_mutated_at > pg_lww_sync.column_timings.last_mutated_at;
              END IF;
            END LOOP;

            IF v_insert_cols != 'id' THEN
              EXECUTE format(
                'INSERT INTO %I.%I (%s) VALUES (%s)',
                p_table_schema, p_table_name, v_insert_cols, v_insert_vals
              );
            END IF;
          ELSE
            -- UPDATE path: per-column LWW
            FOR v_col IN
              SELECT attname FROM pg_attribute
              WHERE attrelid = (quote_ident(p_table_schema) || '.' || quote_ident(p_table_name))::regclass
                AND attnum > 0 AND NOT attisdropped AND attname != 'id'
              ORDER BY attnum
            LOOP
              v_incoming_time := (p_column_timestamps->>v_col)::TIMESTAMP WITH TIME ZONE;
              IF v_incoming_time IS NOT NULL THEN
                SELECT last_mutated_at INTO v_local_time FROM pg_lww_sync.column_timings
                WHERE table_schema = p_table_schema AND table_name = p_table_name
                  AND record_id = p_record_id AND column_name = v_col;

                IF v_local_time IS NULL OR v_incoming_time > v_local_time THEN
                  v_update_sql := v_update_sql || format(', %I = (SELECT %I FROM %I)', v_col, v_col, v_temp_table_name);

                  INSERT INTO pg_lww_sync.column_timings (table_schema, table_name, record_id, column_name, last_mutated_at)
                  VALUES (p_table_schema, p_table_name, p_record_id, v_col, v_incoming_time)
                  ON CONFLICT (table_schema, table_name, record_id, column_name)
                  DO UPDATE SET last_mutated_at = EXCLUDED.last_mutated_at
                  WHERE EXCLUDED.last_mutated_at > pg_lww_sync.column_timings.last_mutated_at;

                ELSIF v_incoming_time = v_local_time THEN
                  IF p_origin_node_id > pg_lww_sync.local_node_id() THEN
                    v_update_sql := v_update_sql || format(', %I = (SELECT %I FROM %I)', v_col, v_col, v_temp_table_name);
                  END IF;
                END IF;
              END IF;
            END LOOP;

            IF v_update_sql != '' THEN
              v_update_sql := ltrim(v_update_sql, ', ');
              EXECUTE format('UPDATE %I.%I SET %s WHERE id = %L', p_table_schema, p_table_name, v_update_sql, p_record_id);
            END IF;
          END IF;
        END;
        $body$ LANGUAGE plpgsql;
      SQL

      ActiveRecord::Base.connection.execute(trigger_sql)
      ActiveRecord::Base.connection.execute("SELECT pg_lww_sync.log_lww_sync_init();")
      ActiveRecord::Base.connection.execute(lww_engine_sql)
    end

    private

    def load_config_file!
      config_path = Rails.root.join("config", "pg_lww_sync.yml")

      unless File.exist?(config_path)
        raise <<~ERROR

          [PgLwwSync] CONFIGURATION ERROR:
          Config file not found at config/pg_lww_sync.yml.
          Run: rails generate pg_lww_sync:install
        ERROR
      end

      raw_yaml = File.read(config_path)
      compiled_yaml = ERB.new(raw_yaml).result
      full_config = YAML.safe_load(compiled_yaml, aliases: true) || {}
      env_config = full_config[Rails.env] || {}

      {
        node_id:      env_config["node_id"],
        remote_nodes: (env_config["remote_nodes"] || []).map(&:symbolize_keys)
      }
    end
  end
end

require_relative "pg_lww_sync/consensus"
require_relative "pg_lww_sync/middleware"
require_relative "pg_lww_sync/replication_client"
require_relative "pg_lww_sync/consumer"
