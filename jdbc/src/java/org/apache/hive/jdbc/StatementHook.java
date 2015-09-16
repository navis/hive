package org.apache.hive.jdbc;

import java.sql.SQLException;

import org.apache.hive.service.cli.thrift.TStatus;

public interface StatementHook {

  void before(String sql);

  void after(String sql, TStatus status);

  void failed(String sql, SQLException ex);
}
