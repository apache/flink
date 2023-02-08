package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultisetToArray extends ScalarFunction {

  public List<String> eval(@DataTypeHint("MULTISET<STRING>") Map<String, Integer> m) {
    if (m == null) {
      return null;
    }

    return new ArrayList<>(m.keySet());
  }
}