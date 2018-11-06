package com.pingcap.join.operator;

import com.pingcap.join.meta.Row;
import com.pingcap.join.meta.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SortMergeJoin extends JoinOperator {
  private Table left;
  private Table right;
  private int cursor = 0;
  private List<Row> result;

  public SortMergeJoin(Table left, Table right) {
    this.left = left;
    this.right = right;
  }

  // if condition is already ordered
  @Override
  public void join() {
    result = new ArrayList<>();
    try (ResultSet innerRows = left.getRows()) {
      try (ResultSet outerRows = right.getRows()) {
        // FIXME: logic bug, rewrite this
        while (true) {
          if (innerRows.next()) {
            if (outerRows.next()) {
              // condition
              if (innerRows.getString(1).equalsIgnoreCase(outerRows.getString(1))
                    && innerRows.getString(2).compareTo(outerRows.getString(2)) > 0) {
                result.add(Row.buildRow(new String[]{innerRows.getString(1), innerRows.getString(2)}));
              } else {
                continue;
              }
            }
            result.add(Row.buildRow(new String[]{innerRows.getString(1), innerRows.getString(2)}));
          } else {
            break;
          }
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Row next() {
    Row row = result.get(cursor);
    cursor++;
    return row;
  }

  @Override
  public void close() {
    // close all resource
    cursor = 0;
  }
}
