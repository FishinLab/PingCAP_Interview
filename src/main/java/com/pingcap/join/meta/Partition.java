package com.pingcap.join.meta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Partition extends Table {

  private long current = 0;
  private static final long FETCH = 100000;
  private static final String SEGMENT = String.format(" limit %d, %d", 0, 10000);

  public Partition(Connection connection, String sql) {
    super(connection, sql + SEGMENT);
  }

  public ResultSet getPartition() {
    ResultSet result = null;
    try {
      result = this.connection.createStatement().executeQuery(super.sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return result;
  }
}
