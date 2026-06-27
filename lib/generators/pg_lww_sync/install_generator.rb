require 'rails/generators/base'
require 'rails/generators/active_record'

module PgLwwSync
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include Rails::Generators::Migration
      source_root File.expand_path('templates', __dir__)

      desc "Installs the PgLwwSync structural database migrations and config/pg_lww_sync.yml network map."

      def self.next_migration_number(dirname)
        ActiveRecord::Generators::Base.next_migration_number(dirname)
      end

      def create_migration_file
        migration_template "create_pg_lww_sync_infrastructure.rb.erb", 
                           "db/migrate/create_pg_lww_sync_infrastructure.rb"
      end

      def create_config_file
        create_file "config/pg_lww_sync.yml", <<~YAML
          # config/pg_lww_sync.yml
          
          development:
            node_id: "dev_node_1"
            remote_nodes: []

          test:
            node_id: "test_node_1"
            remote_nodes: []

          production:
            # Assign a unique identifier for this operational regional database node.
            node_id: "<%= ENV['REPLICATION_NODE_ID'] %>"
            
            # Add database map nodes to scale your mesh cluster without committing code.
            remote_nodes:
              # - node_id: "eu_west_prod"
              #   adapter: "postgresql"
              #   host: "eu-db.yourdomain.internal"
              #   database: "my_app_production"
              #   username: "lww_sync"
              #   password: "<%= ENV['EU_DB_PASSWORD'] %>"
              #   port: 5432
              #   pool: 15
        YAML
      end
    end
  end
end
