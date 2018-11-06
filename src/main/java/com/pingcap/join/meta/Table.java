package com.pingcap.join.meta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Table {
  Connection connection;
  String sql;

  public Table(Connection connection, String sql) {
    this.connection = connection;
    this.sql = sql;
  }

  public ResultSet getRows() throws SQLException {
    return connection.createStatement().executeQuery(sql);
  }

  public Connection getConnection() {
    return this.connection;
  }

  public String getSql() {
    return this.sql;
  }
}
