require 'pg'
require 'json'
require 'tmpdir'

module PgLwwSync
  module Consensus
    # Lazily evaluated so node_id is available at call time, not at require time.
    # Namespaced by node_id so multiple Rails apps on the same host never share
    # the same file and corrupt each other's primary selection.
    def self.state_file
      base = File.directory?("/dev/shm") ? "/dev/shm" : Dir.tmpdir
      File.join(base, "pg_lww_sync_#{PgLwwSync.node_id}.json")
    end

    def self.determine_active_primary(blacklist: [])
      # CLOCK_MONOTONIC is immune to DST changes and NTP adjustments
      current_tick = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i / 5

      begin
        data = read_json_file(state_file)
        if data && data[:tick] == current_tick && data[:primary].is_a?(Hash) &&
           !data[:primary].empty? && !blacklist.include?(data[:primary][:node_id])
          return data[:primary]
        end
      rescue Errno::ENOENT, JSON::ParserError
        # File missing or corrupt — fall through to the safe path
      end

      File.open(state_file, File::RDWR | File::CREAT, 0o644) do |f|
        f.flock(File::LOCK_EX)

        f.rewind
        data = read_json(f)
        if data && data[:tick] == current_tick && data[:primary].is_a?(Hash) &&
           !data[:primary].empty? && !blacklist.include?(data[:primary][:node_id])
          return data[:primary]
        end

        primary = perform_network_health_checks(blacklist: blacklist)

        f.rewind
        f.write({ tick: current_tick, primary: primary }.to_json)
        f.flush
        f.truncate(f.pos)

        primary
      end
    end

    def self.invalidate_fast_path!
      File.open(state_file, File::RDWR | File::CREAT, 0o644) do |f|
        if f.flock(File::LOCK_EX | File::LOCK_NB)
          f.rewind
          f.write({ tick: 0, primary: {} }.to_json)
          f.flush
          f.truncate(f.pos)
        end
      end
    rescue Errno::ENOENT
      nil
    rescue => e
      Rails.logger.warn "[PgLwwSync Consensus] invalidate_fast_path! failed: #{e.message}"
    end

    private

    def self.perform_network_health_checks(blacklist:)
      # Local node connection comes from database.yml via ActiveRecord — no need
      # to duplicate credentials into pg_lww_sync.yml.
      ar_cfg = ActiveRecord::Base.connection_db_config.configuration_hash
      local_node = {
        node_id:  PgLwwSync.node_id,
        host:     ar_cfg[:host] || "localhost",
        port:     ar_cfg[:port] || 5432,
        database: ar_cfg[:database],
        username: ar_cfg[:username],
        password: ar_cfg[:password]
      }

      nodes = ([local_node] + PgLwwSync.remote_nodes).reject { |n| blacklist.include?(n[:node_id]) }
      raise "Cluster Blackout: All nodes are blacklisted or unavailable." if nodes.empty?

      mutex   = Mutex.new
      healthy = []

      threads = nodes.map do |node|
        Thread.new do
          begin
            conn = PG.connect(
              host:            node[:host],
              port:            node[:port],
              dbname:          node[:database],
              user:            node[:username],
              password:        node[:password],
              connect_timeout: 1
            )
            in_recovery = conn.exec("SELECT pg_is_in_recovery();").getvalue(0, 0) == 't'
            conn.close
            mutex.synchronize { healthy << node } unless in_recovery
          rescue => e
            Rails.logger.debug "[PgLwwSync Consensus] Node #{node[:node_id]} unreachable: #{e.message}"
          end
        end
      end

      threads.each(&:join)
      raise "Cluster Blackout: No responsive primary database endpoints found." if healthy.empty?

      # Deterministic: lowest node_id string wins — stable and consistent across all nodes
      healthy.min_by { |n| n[:node_id] }
    end

    def self.read_json_file(path)
      File.open(path, 'r') { |f| read_json(f) }
    end

    def self.read_json(f)
      content = f.read
      return nil if content.strip.empty?
      JSON.parse(content, symbolize_names: true)
    end
  end
end
