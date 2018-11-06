package com.pingcap.join.operator;

import com.pingcap.join.meta.Partition;
import com.pingcap.join.meta.Row;
import com.pingcap.join.meta.Table;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

// not thread safe
public class NestLoopJoin extends JoinOperator {
  private Table left;
  private Table right;
  private List<Row> result;
  private int cursor = 0;

  // TODO: condition
  public NestLoopJoin(Table left, Table right) {
    this.left = left;
    this.right = right;
  }

  public void join() {
    List<Row> result = new ArrayList<>();

    try {
      ResultSet outerRows = left.getRows();
      Partition partition = new Partition(right.getConnection(), right.getSql());
      try (ResultSet innerRows = partition.getRows()) {
        while (innerRows.next()) {
          while (outerRows.next()) {
            // SQL join condition
            if (outerRows.getString(1).equalsIgnoreCase(innerRows.getString(1))
              && outerRows.getString(2).compareTo(innerRows.getString(2)) > 0) {
              //build a new row and add into result
              result.add(Row.buildRow(new String[]{outerRows.getString(1), outerRows.getString(2)}));
            }
          }
          outerRows = left.getRows();
        }
      } catch (Exception error) {
        // inner rows open error
        error.printStackTrace();
      }

    } catch (Exception error) {
      // outer rows open error
      error.printStackTrace();
    }
    this.result = result;
  }

  public Row next() {
    Row row = result.get(cursor);
    cursor++;
    return row;
  }

  public void close() {
    // close resource
    cursor = 0;
  }
}
