package com.pingcap.join.operator;

import com.pingcap.join.meta.Row;

public interface Expression {
  Row next();
  void close();
}

