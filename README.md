# PgLwwSync

`PgLwwSync` is a self-contained, application-level active-active multi-master replication engine designed specifically for **PostgreSQL 13+** and **Ruby on Rails**. 

Unlike native PostgreSQL logical replication—which relies on strict write-ahead log (WAL) decoding and suffers from catastrophic replication halts during write conflicts—`PgLwwSync` leverages an asynchronous application-level transactional outbox pattern combined with a microsecond-accurate **Last-Write-Wins (LWW) Column Clock Matrix** and an automated **Shared-Memory Cluster Routing Layer**. This allows multiple globally distributed database nodes to safely accept write traffic, dynamically survive localized infrastructure outages, and automatically converge to a globally consistent state.

## Key Features
* **Zero-Infrastructure Multi-Master Mesh:** No external message brokers (Kafka, RabbitMQ) or invasive database extensions (`pglogical`, `BDR`) required. It establishes communication lines using standard database connection configurations.
* **Granular Column-Level Conflict Resolution:** Last-Write-Wins (LWW) resolution rules are tracked at the **individual column level**, minimizing data loss and eliminating row-level clobbering during cross-region concurrent updates.
* **Shared-Memory Consensus Routing:** Features a cross-process cluster coordinator that dynamically routes client queries to the optimal healthy database instance, refreshing statuses every 5 seconds via `/dev/shm`.
* **Seamless Request Failover-Retry Middleware:** Transparently intercepts connection dropouts or node crashes inside the web middleware layer (`RequestRouter`), instantly blacklists the dead node, clears shared states, and transparently retries the user's transaction against surviving peers.
* **Multi-Schema Auto-Discovery:** Dynamically discovers, monitors, and links operational application tables while strictly ignoring internal system catalogs, schemas, and tracking definitions.
* **Resilient Connection Fault-Tolerance:** Structural network partitions or extended remote server down-times do not drop transactional history. Records sit safely inside local outbox tables until connection integrity recovers.

---

## Architecture Blueprint

```
                     +---------------------------------------+
                     |         HTTP / Client Request         |
                     +---------------------------------------+
                                         |
                                         v
                     +---------------------------------------+
                     |      PgLwwSync::RequestRouter         |
                     +---------------------------------------+
                                         |
               +-------------------------+-------------------------+
               | (Reads Consensus from /dev/shm/pg_lww_sync.json)  |
               v                                                   v
    [ Primary Node Alive? ]                             [ Primary Node Crashed? ]
               |                                                   |
               v                                                   v
   +-----------------------+                         +---------------------------+
   | Proxies connection to |                         | 1. Deletes SHM state file |
   | designated target db  |                         | 2. Health-checks cluster  |
   +-----------------------+                         | 3. Blacklists dead node   |
               |                                     | 4. Transparently retries  |
               v                                     +---------------------------+
   +-----------------------+                                       |
   | Executes application  |                                       v
   | mutation payload code |                         +---------------------------+
   +-----------------------+                         | Re-routes thread pool to  |
               |                                     | the next healthy peer     |
               v                                     +---------------------------+
+-----------------------------+                                    |
| PL/pgSQL Trigger Log Engine | <----------------------------------+
+-----------------------------+
               |
               v (Appends transaction rows)
+-----------------------------+
| pg_lww_changesets (Outbox)  |
+-----------------------------+
               |
               v (Polled asynchronously by Transactional Buckets)
+-----------------------------+
|    Background Consumer      | ====> Broadcasts payload using Replica Role
+-----------------------------+       to Remote Peer Matrix Clocks
```

---

## Requirements

* **Ruby:** 3.0+
* **Ruby on Rails:** 6.1+
* **Database:** PostgreSQL 13.0+ (Utilizes native JSONB operations, transactional horizons, advisory locking, and `session_replication_role`).

---

## Installation & Initial Setup

Add the gem directly to your application's `Gemfile`:

```ruby
gem 'pg_lww_sync'
```

Execute your bundle environment alignment commands:

```bash
bundle install
```

### Zero-Configuration Middleware Activation
You **do not** need to manually modify your `config/application.rb` file to inject or register the replication middleware stack. `PgLwwSync` utilizes a native Rails Railtie engine to automatically bootstrap itself into your application's middleware pipeline on boot.

The engine hooks into the framework initialization lifecycle and automatically mounts `PgLwwSync::RequestRouter` directly before `ActiveRecord::Migration::CheckPending`. This guarantees that cluster consensus topology calculations and destination primary routing are resolved seamlessly before Rails evaluates pending schema migration states or handles active request processing loops.

### Setup System Schemas
Generate the cluster networking mapping configurations and system-level database schemas:

```bash
rails generate pg_lww_sync:install
```

Run the generated database migration to create the core synchronization infrastructure tables (`pg_lww_changesets` and `column_timings`):

```bash
rails db:migrate
```

---

## Core Cluster Topology Configuration

The installer establishes an environment map structure inside `config/pg_lww_sync.yml`. You must define a unique `node_id` for each region and expose database credentials pointing to every peer node across your infrastructure matrix.

```yaml
# config/pg_lww_sync.yml
production:
  # Assign a unique structural identifier for this local regional node instance
  node_id: "us_east_primary"
  
  # Register all cross-region database peer targets making up your active-active mesh
  remote_nodes:
    - node_id: "eu_west_primary"
      adapter: "postgresql"
      host: "eu-database.yourdomain.internal"
      database: "production_application_db"
      username: "lww_sync_replication"
      password: "<%= ENV['EU_DB_PASSWORD'] %>"
      port: 5432
      pool: 15
      
    - node_id: "ap_south_primary"
      adapter: "postgresql"
      host: "ap-database.yourdomain.internal"
      database: "production_application_db"
      username: "lww_sync_replication"
      password: "<%= ENV['AP_DB_PASSWORD'] %>"
      port: 5432
      pool: 15
```

---

## Understanding the Engine Architecture

### 1. The Consensus Engine (`PgLwwSync::Consensus`)
To protect against race conditions across distributed multi-process environments (like multiple Puma or Unicorn workers running on the same server), the consensus system implements a hybrid **Fast-Path / Lock-Protected** shared-memory strategy:
* **Fast Path:** Workers check an in-memory JSON state representation at `/dev/shm/pg_lww_sync.json`. If it matches the current 5-second interval tick, the request skips the network and routes instantly to the cached primary.
* **Safe Path:** If the cache expires, workers trigger a standard POSIX file-lock (`flock`). The single worker holding the lock spins up background check threads to verify cluster health and confirm `pg_is_in_recovery()` states, saving the newest primary to shared memory for everyone else to consume.

### 2. The Failover Routing Loop (`RequestRouter`)
The custom HTTP request processor (`PgLwwSync::RequestRouter`) completely automates multi-node database fault-tolerance:
* **Dynamic Target Routing:** For every incoming request, it query-checks the consensus layer and wraps the active thread pool context around the chosen target instance using a custom-named dynamic pool connection handler (`pg_sync_pool_<node_id>`).
* **Automated Node Outage Interception:** If a database node drops mid-execution or crashes before a transaction commits, the middleware intercepts the lower-level connection error (`PG::ConnectionBad`, `ActiveRecord::ConnectionNotEstablished`, or connection-dropped `StatementInvalid`).
* **Instant Blacklist & Recovery:** The middleware immediately wipes the shared memory status file `/dev/shm/pg_lww_sync.json` to flag a failure event to the machine. It blacklists the failed node ID for the remainder of that specific request lifecycle, re-runs cluster calculations, hooks into a surviving healthy database peer, and **re-tries the transaction transparently** without throwing an error page back to your users.

### 3. Microsecond-Accurate Last-Write-Wins (LWW) Matrix with Clock-Drift Fix
When records undergo updates, a low-level native PL/pgSQL trigger writes an operational change manifest to the outbox ledger. The conflict engine processes mutations column-by-column against a localized high-precision matrix (`pg_lww_sync.column_timings`) using a robust multi-tiered validation approach:
* **Chronological Comparison:** Columns that are untouched by an operation retain their local values. If an incoming microsecond timestamp is strictly newer than the recorded local mutation timestamp, the update applies and overwrites the column values. Historical timestamps are safely discarded to avoid row-wide clobbering.
* **Deterministic Sorting Tie-Breaker:** If the incoming timestamp is an exact match to the local timestamp (`v_incoming_time = v_local_time`)—which frequently occurs when cross-region servers encounter a microsecond tie or physical clock drift overlaps—the engine eliminates randomness and divergence. It compares the originating server's identifier (`p_origin_node_id`) against the destination node's ID (`pg_lww_sync.local_node_id()`) using a strict lexicographical string sort. Because all nodes evaluate this alphabetical rule identically, race conditions are averted and the active-active mesh naturally converges on a globally consistent value.

### 4. Background Transaction Buckets (Consumer Execution) & Atomicity Preservation
The asynchronous replication daemon processes outbound changesets by parsing the ledger outbox and grouping rows cleanly by their native database `transaction_id`.
* **Parallel Transaction Distribution:** Unique transaction IDs are assigned across worker threads using a deterministic modulo strategy (`index % pool_size`), routing rows into separate thread-safe memory channels (`@queues`).
* **Atomicity Maintenance:** Whole transactions are wrapped within an isolated database block (`remote_conn.transaction`) on the destination peer. This guarantees that multi-row mutations are evaluated as a single atomic element, preventing partial data leaks and protecting relational database foreign-key constraints on remote targets.

---

## Advanced Self-Healing Mechanics

### 1. Transaction Horizon Deadlock Protection
In its default high-performance path, the background consumer isolates committed data boundaries using a database transaction snapshot low-water mark strategy (`transaction_id < txid_snapshot_xmin(txid_current_snapshot())`) to ensure it only reads safely committed rows.

#### The Pitfall
Because PostgreSQL relies on Multi-Version Concurrency Control (MVCC), the low-water mark (`xmin`) of a transaction snapshot is bound to the *oldest currently active transaction across the entire database server*. If an unrelated workflow—such as a long-running data migration, a heavy analytical report, or an unclosed manual SQL console session—stalls on the database node, the global `xmin` horizon freezes. This paralyzes the standard replication stream, causing outbox ledger records to accumulate and replication lag to spike linearly.

#### The Circuit-Breaker Cache Strategy
To protect against replication paralysis, the consumer features an automated **Decoupled State Polling Backoff** engine. Every 10 seconds, the consumer worker audits `pg_stat_activity` to inspect the age of active database blocks. If a frozen transaction horizon exceeds your configured ceiling limit (`max_horizon_age_seconds: 60`), the consumer trips a local circuit breaker and dynamically pivots its lookup strategy to a **Sliding Time-Window Filter**:

```sql
GROUP BY transaction_id
HAVING MAX(committed_at) < (clock_timestamp() - interval '5 seconds')
```

This bypasses the frozen MVCC snapshot boundary entirely, allowing active replication to flow around the stalled transaction without skipping a beat.

### 2. Guarding Fallback Transaction Split Boundaries
When the background replication daemon shifts into the sliding time-window filter fallback mode, a multi-row transaction could have individual changesets written across a microsecond boundary that straddles the cutoff edge. 

`PgLwwSync` handles this using a **Windowed Subquery Aggregation Layer**. During fallback, the consumer groups rows through an explicit `HAVING MAX(committed_at) < (clock_timestamp() - interval '5 seconds')` filter. By checking that the *latest* written component of an entire transaction group is safely older than the safety cutoff interval, it guarantees that a transaction ID is never split across two separate replication loops.

### 3. Poison Pill Deadlock Isolation (Auto-Quarantine State)
When outbound changesets encounter structural exceptions (e.g., missing columns or structural datatype mismatches) on a target peer node, traditional replication drivers would freeze completely, crashing the worker thread and stalling the sync loop for all other unrelated application tables until manual engineering intervention occurred.

`PgLwwSync` resolves this by employing an atomic **Poison Pill Auto-Quarantine Strategy**:
* **Stream Non-Blocking:** When a transaction payload fails to commit due to database schema errors, the daemon catches the exception, updates the local outbox tracking table state row elements to `status = 'failed'`, and logs which node caused the rejection.
* **Seamless Bypassing:** Future fetch ticks look strictly for `WHERE status = 'pending'`, skipping the problematic transaction entirely. This allows updates on healthy tables to continue flowing smoothly across your distributed cluster without incurring replication lag.

---

## Production Maintenance & Rake Tasks

### Replaying Failed Quarantined Transactions
Once you address structural schema alignment differences on a lagging node (by running the necessary database migration), you can instruct `PgLwwSync` to flush and retry the quarantined outbox rows back into active synchronization pipeline processing by running:

```bash
bundle exec rake pg_lww_sync:replay_failed
```

### Manual Realignment Triggering
If you perform extensive manual DDL operations, deploy massive schema changes outside normal ActiveRecord migrations, or introduce new structural tables, you can force a cold recompile and realign tracking vectors across all schemas by running:

```bash
bundle exec rake pg_lww_sync:realign
```

---

## Troubleshooting & Custom Error Reporting Callback

`PgLwwSync` does not force an opinionated error tracking dependency onto your application, nor does it ask developers to manually audit database log tables for replication faults. Structural and data-type synchronization anomalies bubble up into Ruby naturally from the target database driver.

### 1. Core Structural Crash Logging
If an outbound replication batch fails due to an out-of-sync regional schema state (e.g., a rolling deployment lag where a newly added column hasn't been migrated to a remote peer node yet), the background daemon formats a prominent log alert inside your standard application stream (`Rails.logger.error`):

```
=== [PgLwwSync Poison Pill Quarantined] ===
Target Peer Node: eu_west_primary
Replication Path: public.users (ID: 92831)
Engine Action:    UPDATE
Exception Class:  ActiveRecord::StatementInvalid
Error Details:    PG::UndefinedColumn: ERROR: column "discount_tier" does not exist
STATUS:           Transaction 5122134 marked as 'failed'. Stream bypassing safely.
===========================================
```

### 2. Configuring Custom Error Tracker Callbacks
You can intercept replication errors and route them directly to your preferred crash monitoring stack (Airbrake, Errbit, Rollbar, Appsignal, Sentry, or internal Slack webhooks) by registering a custom configuration block.

Create an initializer file at `config/initializers/pg_lww_sync.rb` and define an `on_failure` block handler:

```ruby
# config/initializers/pg_lww_sync.rb
PgLwwSync.on_failure do |exception, context|
  # context is a hash containing:
  # :target_node_id, :failed_schema, :failed_table, :primary_record_id, :action_type, :transaction_id

  if defined?(Airbrake)
    Airbrake.notify(exception, parameters: context)
  elsif defined?(Rollbar)
    Rollbar.error(exception, context)
  end
  
  # Example: Pipe alerts directly into an internal developer Slack channel
  SlackNotifier.ping(
    "⚠️ *Replication Poison Pill Quarantined on #{context[:target_node_id]}*: " \
    "Failed to sync table `#{context[:failed_table]}` for Record ID: #{context[:primary_record_id]}. " \
    "Transaction has been bypassed safely."
  )
end
```

### 3. Common Structural Synchronization Anomalies
When configuring alert routing logic or troubleshooting issues caught by your callback block, keep an eye out for these typical multi-region infrastructure events:
* **`PG::UndefinedColumn`:** Occurs during staggered production releases. Node A streams a changeset containing a newly migrated column attribute to Node B before Node B has finished executing its corresponding `rails db:migrate` sequence.
* **`PG::DatatypeMismatch`:** Triggered if database column type updates diverge or are misaligned between regional clusters (e.g., attempting to stream alphanumeric characters into a field that a peer node still enforces as an integer scalar).
* **`PG::StringDataRightTruncation`:** Occurs when character threshold boundaries differ across target nodes (e.g., Node A saves a 120-character string into a field defined as `text`, but Node B rejects it because its local table configuration still binds that field to a restricted `varchar(50)` limit constraint).

**Resolution Procedure:** Once you receive a schema mismatch alert from your tracker, simply bring the target node's database catalog into alignment by running the missing migration setup steps. Once aligned, run `rake pg_lww_sync:replay_failed` to clear out the quarantine queue.

---

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
```