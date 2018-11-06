package com.pingcap.join.operator;

import com.pingcap.join.meta.Row;
import com.pingcap.join.meta.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashJoin extends JoinOperator {
  private Table left;
  private Table right;
  private Map<String, String> hashTable;
  private List<Row> result;
  private int cursor = 0;

  public HashJoin(Table left, Table right) {
    this.left = left;
    this.right = right;
  }

  public void join() {
    hashTable = new HashMap<>();
    result = new ArrayList<>();
    try (ResultSet outerRows = left.getRows()) {
      while (outerRows.next()) {
        hashTable.put(outerRows.getString(1), outerRows.getString(2));
      }

      try (ResultSet innerRows = right.getRows()) {
        while (innerRows.next()) {
          String key = innerRows.getString(1);
          String val = innerRows.getString(2);
          // condition
          if (hashTable.containsKey(key) && hashTable.get(key).compareTo(val) > 0) {
            result.add(Row.buildRow(new String[]{key, val}));
          }
        }
      } catch (SQLException e) {
        // open inner rows error
        e.printStackTrace();
      }
    } catch (SQLException e) {
      // open outer rows error
      e.printStackTrace();
    }
  }

  public Row next() {
    Row row = result.get(cursor);
    cursor++;
    return row;
  }

  public void close() {
    // close all resource
    cursor = 0;
  }
}
