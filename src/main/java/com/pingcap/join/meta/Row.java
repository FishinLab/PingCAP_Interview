package com.pingcap.join.meta;

import java.sql.Connection;

public class Row {
  private String sql;
  private Connection jdbc;

  private String[] values;

  public static Row buildRow(String[] values) {
    return new Row(values);
  }

  public String getValue(int index) {
    if (index < values.length) {
      return values[index];
    } else {
      return null;
    }
  }

  public Row(String[] values) {
    this.values = values;
  }
}
