package com.pingcap.join.DataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class JdbcDataSource {
  private Connection conn;
  private final String url = "";
  private final String user = "";
  private final String password = "";
  public static final String ENGINE = "MySQL";
  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  public final Connection getConnection() throws Exception {
    if (conn == null) {
      synchronized (this) {
        if (conn == null) {
          Class.forName(JDBC_DRIVER);
          conn = DriverManager.getConnection(url, user, password);
        }
      }
    }
    return conn;
  }

  public ResultSet next(String sql) {
    ResultSet set = null;
    try {
      Connection conn = getConnection();
      set = conn.createStatement().executeQuery(sql);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return set;
  }
}
