#!/usr/bin/env sysbench

-- Keep sysbench's standard oltp_read_write run behavior, but optionally use a
-- Hamstergres-aware prepare path. The stock PostgreSQL prepare path creates the
-- table and then emits the complete dataset as one multi-row INSERT. A sharded
-- write must have exactly one shard-key tuple, so the sharded path below marks
-- the key immediately after CREATE TABLE and loads one keyed row per INSERT.
require("oltp_read_write")

sysbench.cmdline.options.hamstergres_sharding = {
  "Declare sbtest<N>.id as a Hamstergres shard key before loading rows", false
}

local sysbench_create_table = create_table

function create_table(driver, connection, table_number)
  if not sysbench.opt.hamstergres_sharding then
    return sysbench_create_table(driver, connection, table_number)
  end

  if driver:name() ~= "pgsql" then
    error("Hamstergres sharded prepare requires sysbench's pgsql driver")
  end
  if sysbench.opt.auto_inc then
    error("Hamstergres sharded prepare requires --auto-inc=off")
  end
  if sysbench.opt.secondary then
    error("Hamstergres sharded prepare does not support --secondary=on")
  end

  print(string.format("Creating sharded table 'sbtest%d'...", table_number))
  connection:query(string.format([[
CREATE TABLE sbtest%d(
  id INTEGER NOT NULL,
  k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL,
  PRIMARY KEY (id)
)]], table_number))
  connection:query(string.format(
    "COMMENT ON COLUMN sbtest%d.id IS 'hamstergres.shard_key'",
    table_number))

  if sysbench.opt.table_size > 0 then
    print(string.format("Inserting %d routed records into 'sbtest%d'",
      sysbench.opt.table_size, table_number))
  end
  for id = 1, sysbench.opt.table_size do
    connection:query(string.format(
      "INSERT INTO sbtest%d (id, k, c, pad) VALUES (%d, %d, '%s', '%s')",
      table_number, id, sb_rand(1, sysbench.opt.table_size),
      get_c_value(), get_pad_value()))
  end

  if sysbench.opt.create_secondary then
    print(string.format("Creating a secondary index on 'sbtest%d'...",
      table_number))
    connection:query(string.format(
      "CREATE INDEX k_%d ON sbtest%d(k)", table_number, table_number))
  end
end
