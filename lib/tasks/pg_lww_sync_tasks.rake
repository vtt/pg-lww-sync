namespace :pg_lww_sync do
  desc "Recompile PL/pgSQL functions and reattach sync triggers to all tables."
  task realign: :environment do
    puts "Refreshing PgLwwSync PL/pgSQL routines..."
    PgLwwSync.initialize_database_functions!

    puts "Scanning tables and attaching triggers..."
    PgLwwSync.sync_all_tables!

    puts "PgLwwSync realignment complete."
  end

  desc "Show counts of pending / failed / success changesets."
  task status: :environment do
    rows = ActiveRecord::Base.connection.select_all(<<~SQL).to_a
      SELECT status, COUNT(*) AS count
      FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
      GROUP BY status
      ORDER BY status;
    SQL

    puts "\nPgLwwSync outbox status:"
    rows.each { |r| puts "  #{r['status'].ljust(10)} #{r['count']}" }
    puts
  end
end
