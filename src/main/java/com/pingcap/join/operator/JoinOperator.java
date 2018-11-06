package com.pingcap.join.operator;

public abstract class JoinOperator implements Expression {
  public enum JOIN_TYPE{
    HASH,
    NEST_LOOP;
  }

  public abstract void join();
}