#!/usr/bin/env sysbench

sysbench.cmdline.options = {
  table_size = {"Number of rows in the benchmark table", 10000}
}

local table_name = "hamstergres_cpu_scaling"

function prepare()
  local driver = sysbench.sql.driver()
  local connection = driver:connect()

  connection:query("DROP TABLE IF EXISTS " .. table_name)
  connection:query("CREATE TABLE " .. table_name .. " (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)")
  connection:query("COMMENT ON COLUMN " .. table_name .. ".id IS 'hamstergres.shard_key'")

  for id = 1, sysbench.opt.table_size do
    connection:query(string.format(
      "INSERT INTO %s (id, payload) VALUES (%d, 'hamster-%d')",
      table_name, id, id))
  end

  connection:disconnect()
end

function cleanup()
  local driver = sysbench.sql.driver()
  local connection = driver:connect()
  connection:query("DROP TABLE IF EXISTS " .. table_name)
  connection:disconnect()
end

function thread_init()
  driver = sysbench.sql.driver()
  connection = driver:connect()
  statement = connection:prepare(
    "SELECT payload FROM " .. table_name .. " WHERE id = ?")
  id_parameter = statement:bind_create(sysbench.sql.type.BIGINT)
  statement:bind_param(id_parameter)
end

function thread_done()
  statement:close()
  connection:disconnect()
end

function event()
  id_parameter:set(sysbench.rand.default(1, sysbench.opt.table_size))
  statement:execute()
end
