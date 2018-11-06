package com.pingcap.join.operator;

public abstract class JoinOperator implements Expression {
  public enum JOIN_TYPE{
    HASH,
    SORT_MERGE,
    NEST_LOOP
  }

  public abstract void join();
}