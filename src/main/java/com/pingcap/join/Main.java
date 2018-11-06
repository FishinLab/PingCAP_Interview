package com.pingcap.join;

import com.pingcap.join.DataSource.JdbcDataSource;
import com.pingcap.join.meta.Row;
import com.pingcap.join.meta.Table;
import com.pingcap.join.operator.HashJoin;
import com.pingcap.join.operator.JoinOperator;
import com.pingcap.join.operator.NestLoopJoin;
import com.pingcap.join.operator.SortMergeJoin;

public class Main {
  private static final String NATIVE_SQL = "select count(*) from dbfree_instance_list list join dbfree_instance_status status " +
                                             "on list.ip = status.ip " +
                                             "and list.gmt_create > status.gmt_create;";

  private static final String LEFT_SQL = "select ip, gmt_create from dbfree_instance_list";

  private static final String RIGHT_SQL = "select ip, gmt_create from dbfree_instance_status";

  public void main(String[] args) throws Exception {
    nestLoopJoin();
    hashJoin();
  }

  private JoinOperator getJoin(JoinOperator.JOIN_TYPE joinType) throws Exception {
    JdbcDataSource leftDataSource = new JdbcDataSource();
    JdbcDataSource rightDataSource = new JdbcDataSource();

    Table left = new Table(leftDataSource.getConnection(), LEFT_SQL);
    Table right = new Table(rightDataSource.getConnection(), RIGHT_SQL);
    if (joinType == JoinOperator.JOIN_TYPE.HASH) {
      return new HashJoin(left, right);
    } else if (joinType == JoinOperator.JOIN_TYPE.NEST_LOOP) {
      return new NestLoopJoin(left, right);
    } else if (joinType == JoinOperator.JOIN_TYPE.SORT_MERGE) {
      return new SortMergeJoin(left, right);
    } else {
      return null;
    }
  }

  private void nestLoopJoin() throws Exception {
    JoinOperator operator = getJoin(JoinOperator.JOIN_TYPE.NEST_LOOP);

    assert operator != null;
    operator.join();

    count(operator);
  }

  private void hashJoin() throws Exception {
    JoinOperator operator = getJoin(JoinOperator.JOIN_TYPE.HASH);

    assert operator != null;
    operator.join();

    count(operator);
  }

  private void count(JoinOperator operator) {
    int count = 0;

    do {
      Row row = operator.next();
      if (row == null) {
        break;
      }
      count++;
    } while (true);

    System.out.println(String.format("count: %d", count));
  }
}
