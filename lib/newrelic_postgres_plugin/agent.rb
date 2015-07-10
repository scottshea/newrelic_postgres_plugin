module NewRelic::PostgresPlugin

  # Register and run the agent
  def self.run
    # Register this agent.
    NewRelic::Plugin::Setup.install_agent :postgres, self

    # Launch the agent; this never returns.
    NewRelic::Plugin::Run.setup_and_run
  end


  class Agent < NewRelic::Plugin::Agent::Base

    agent_guid    'com.scottshea.postgres'
    agent_version NewRelic::PostgresPlugin::VERSION
    agent_config_options :host, :port, :user, :password, :dbname, :sslmode, :label
    agent_human_labels('Postgres') { "#{label || host}" }

    def initialize(*args)
      @previous_metrics = {}
      @previous_result_for_query ||= {}
      super
    end

    #
    # Required, but not used
    #
    def setup_metrics
    end

    #
    # You do not have to specify the postgres port in the yaml if you don't want to.
    #
    def port
      @port || 5432
    end

    #
    # Get a connection to postgres
    #
    def connect
      PG::Connection.new(:host => host, :port => port, :user => user, :password => password, :sslmode => sslmode, :dbname => dbname)
    end

    #
    # Returns true if we're talking to Postgres version >= 9.2
    #
    def nine_two?
      @connection.server_version >= 90200
    end

    #
    # This is called on every polling cycle
    #
    def poll_cycle
      @connection = self.connect

      report_metrics

    rescue => e
      $stderr.puts "#{e}: #{e.backtrace.join("\n  ")}"
    ensure
      @connection.finish if @connection
    end

    def report_metrics
      @connection.exec(backend_query) do |result|
        report_metric 'Backends/Active', 'connections', result[0]['backends_active']
        report_metric 'Backends/Idle',   'connections', result[0]['backends_idle']
      end
      @connection.exec(database_query) do |result|
        result.each do |row|
          report_metric         'Database/Backends',                 'count', row['numbackends'].to_i
          report_derived_metric 'Database/Transactions/Committed',   'transactions', row['xact_commit'].to_i
          report_derived_metric 'Database/Transactions/Rolled Back', 'transactions', row['xact_rollback'].to_i

          report_derived_metric 'Database/Rows/Selected', 'rows', row['tup_returned'].to_i + row['tup_fetched'].to_i
          report_derived_metric 'Database/Rows/Inserted', 'rows', row['tup_inserted'].to_i
          report_derived_metric 'Database/Rows/Updated',  'rows', row['tup_updated'].to_i
          report_derived_metric 'Database/Rows/Deleted',  'rows', row['tup_deleted'].to_i

        end
      end
      @connection.exec(index_count_query) do |result|
        report_metric 'Database/Indexes/Count', 'indexes', result[0]['indexes'].to_i
      end
      @connection.exec(index_size_query) do |result|
        report_metric 'Database/Indexes/Size', 'bytes', result[0]['size'].to_i
      end
      @connection.exec(bgwriter_query) do |result|
        report_derived_metric 'Background Writer/Checkpoints/Scheduled', 'checkpoints', result[0]['checkpoints_timed'].to_i
        report_derived_metric 'Background Writer/Checkpoints/Requested', 'checkpoints', result[0]['checkpoints_requests'].to_i
      end
      report_metric 'Alerts/Index Miss Ratio', '%', calculate_miss_ratio(%Q{SELECT SUM(idx_blks_hit) AS hits, SUM(idx_blks_read) AS reads FROM pg_statio_user_indexes})
      report_metric 'Alerts/Cache Miss Ratio', '%', calculate_miss_ratio(%Q{SELECT SUM(heap_blks_hit) AS hits, SUM(heap_blks_read) AS reads FROM pg_statio_user_tables})


      @connection.exec(database_bloat).each do |row|
       next if row['schemaname'] == 'pg_catalog'
       type = row['type'].capitalize
       name = row['object_name']
       report_metric "Table/Waste/#{type}/#{name}", 'bloat', row['bloat'].to_f
       report_metric "Table/Waste/#{type}/#{name}", 'bytes', row['waste'].to_i
      end

      @connection.exec(seq_scans).each do |row|
        report_metric "Table/Seq Scans/#{row['name']}", 'count', row['count'].to_i
      end

      @connection.exec(vacuum_stats).each do |row|
        name = row['table']
        report_metric "Table/Row Count/#{name}", 'count', row['rowcount'].to_i
        report_metric "Table/Dead Row Count/#{name}", 'count', row['dead_rowcount'].to_i
      end

      @connection.exec(records_rank).each do |row|
        report_metric "Table/Records Rank/#{row['name']}", 'count', row['estimated_count'].to_i
      end


      # This is dependent on the pg_stat_statements being loaded, and assumes that pg_stat_statements.max has been set sufficiently high that most queries will be recorded. If your application typically generates more than 1000 distinct query plans per sampling interval, you're going to have a bad time.
      if extension_loaded? 'pg_stat_statements'
        @connection.exec('SELECT SUM(calls) FROM pg_stat_statements') do |result|
          report_derived_metric 'Database/Statements', '', result[0]['sum'].to_i
        end
      else
        puts 'pg_stat_statements is not loaded; no Database/Statements metric will be reported.'
      end
    end

    private

      def extension_loaded?(extname)
        @connection.exec("SELECT count(*) FROM pg_extension WHERE extname = '#{extname}'") do |result|
          result[0]['count'] == '1'
        end
      end

      def report_derived_metric(name, units, value)
        if previous_value = @previous_metrics[name]
          report_metric name, units, (value - previous_value)
        else
          report_metric name, units, 0
        end
        @previous_metrics[name] = value
      end

      # This assumes the query returns a single row with two columns: hits and reads.
      def calculate_miss_ratio(query)
        sample = @connection.exec(query)[0]
        sample.each { |key,value| sample[key] = value.to_i }
        miss_ratio = if check_samples(@previous_result_for_query[query], sample)

          hits = sample['hits'] - @previous_result_for_query[query]['hits']
          reads = sample['reads'] - @previous_result_for_query[query]['reads']
          
          if (hits + reads) == 0
            0.0
          else
            reads.to_f / (hits + reads) * 100.0
          end
        else
          0.0
        end
      
        @previous_result_for_query[query] = sample
        return miss_ratio
      end
 
      # Check if we don't have a time dimension yet or metrics have decreased in value.
      def check_samples(last, current)
        return false if last.nil? # First sample?
        return false unless current.find { |k,v| last[k] > v }.nil? # Values have gone down?
        return true
      end

      def backend_query
        %Q(
          SELECT ( SELECT count(*) FROM pg_stat_activity WHERE
            #{
              if nine_two?
                "state <> 'idle'"
              else
                "current_query <> '<IDLE>'"
              end
            }
          ) AS backends_active, ( SELECT count(*) FROM pg_stat_activity WHERE
            #{
              if nine_two?
                "state = 'idle'"
              else
                "current_query = '<IDLE>'"
              end
            }
          ) AS backends_idle FROM pg_stat_activity;
        )
      end

      def database_query
        "SELECT * FROM pg_stat_database WHERE datname='#{dbname}';"
      end

      def bgwriter_query
        'SELECT * FROM pg_stat_bgwriter;'
      end

      def index_count_query
        "SELECT count(1) as indexes FROM pg_class WHERE relkind = 'i';"
      end

      def index_size_query
        'SELECT sum(relpages::bigint*8192) AS size FROM pg_class WHERE reltype = 0;'
      end

      def seq_scans
        'SELECT relname AS name, seq_scan as count FROM pg_stat_user_tables ORDER BY seq_scan DESC;'
      end

      def records_rank
        'SELECT relname AS name, n_live_tup AS estimated_count FROM pg_stat_user_tables ORDER BY n_live_tup DESC;'
      end

      def database_bloat
        %q(
          WITH constants AS (
                    SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 4 AS ma
                  ), bloat_info AS (
                    SELECT
                      ma,bs,schemaname,tablename,
                      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
                      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
                    FROM (
                      SELECT
                        schemaname, tablename, hdr, ma, bs,
                        SUM((1-null_frac)*avg_width) AS datawidth,
                        MAX(null_frac) AS maxfracsum,
                        hdr+(
                          SELECT 1+count(*)/8
                          FROM pg_stats s2
                          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
                        ) AS nullhdr
                      FROM pg_stats s, constants
                      GROUP BY 1,2,3,4,5
                    ) AS foo
                  ), table_bloat AS (
                    SELECT
                      schemaname, tablename, cc.relpages, bs,
                      CEIL((cc.reltuples*((datahdr+ma-
                        (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta
                    FROM bloat_info
                      JOIN pg_class cc ON cc.relname = bloat_info.tablename
                      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
                  ), index_bloat AS (
                    SELECT
                      schemaname, tablename, bs,
                      COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
                      COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
                    FROM bloat_info
                      JOIN pg_class cc ON cc.relname = bloat_info.tablename
                      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
                      JOIN pg_index i ON indrelid = cc.oid
                      JOIN pg_class c2 ON c2.oid = i.indexrelid
                  )
                  SELECT
                    type, schemaname, object_name, bloat, pg_size_pretty(raw_waste) as waste
                  FROM
                  (SELECT
                    'table' as type,
                    schemaname,
                    tablename as object_name,
                    ROUND(CASE WHEN otta=0 THEN 0.0 ELSE table_bloat.relpages/otta::numeric END,1) AS bloat,
                    CASE WHEN relpages < otta THEN '0' ELSE (bs*(table_bloat.relpages-otta)::bigint)::bigint END AS raw_waste
                  FROM
                    table_bloat
                      UNION
                  SELECT
                    'index' as type,
                    schemaname,
                    tablename || '::' || iname as object_name,
                    ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat,
                    CASE WHEN ipages < iotta THEN '0' ELSE (bs*(ipages-iotta))::bigint END AS raw_waste
                  FROM
                    index_bloat) bloat_summary
                  ORDER BY raw_waste DESC, bloat DESC
        )
      end

      def vacuum_stats
        %q(
          WITH table_opts AS (
            SELECT
              pg_class.oid, relname, nspname, array_to_string(reloptions, '') AS relopts
            FROM
               pg_class INNER JOIN pg_namespace ns ON relnamespace = ns.oid
          ), vacuum_settings AS (
            SELECT
              oid, relname, nspname,
              CASE
                WHEN relopts LIKE '%autovacuum_vacuum_threshold%'
                  THEN regexp_replace(relopts, '.*autovacuum_vacuum_threshold=([0-9.]+).*', E'\\\\\\1')::integer
                  ELSE current_setting('autovacuum_vacuum_threshold')::integer
                END AS autovacuum_vacuum_threshold,
              CASE
                WHEN relopts LIKE '%autovacuum_vacuum_scale_factor%'
                  THEN regexp_replace(relopts, '.*autovacuum_vacuum_scale_factor=([0-9.]+).*', E'\\\\\\1')::real
                  ELSE current_setting('autovacuum_vacuum_scale_factor')::real
                END AS autovacuum_vacuum_scale_factor
            FROM
              table_opts
          )
          SELECT
            vacuum_settings.nspname AS schema,
            vacuum_settings.relname AS table,
            to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') AS last_vacuum,
            to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
            to_char(pg_class.reltuples, '9G999G999G999') AS rowcount,
            to_char(psut.n_dead_tup, '9G999G999G999') AS dead_rowcount,
            to_char(autovacuum_vacuum_threshold
                 + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples), '9G999G999G999') AS autovacuum_threshold,
            CASE
              WHEN autovacuum_vacuum_threshold + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples) < psut.n_dead_tup
              THEN 'yes'
            END AS expect_autovacuum
          FROM
            pg_stat_user_tables psut INNER JOIN pg_class ON psut.relid = pg_class.oid
              INNER JOIN vacuum_settings ON pg_class.oid = vacuum_settings.oid
          ORDER BY 1
       )
      end

  end

end
