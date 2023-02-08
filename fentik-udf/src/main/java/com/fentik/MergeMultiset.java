package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.Map;
import java.util.HashMap;

public class MergeMultiset extends ScalarFunction {

  public @DataTypeHint("MULTISET<STRING>") Map<String, Integer> eval(
      @DataTypeHint("MULTISET<STRING>") Map<String, Integer> m1,
      @DataTypeHint("MULTISET<STRING>") Map<String, Integer> m2) {

    if (m1 == null && m2 == null) {
      return null;
    }

    Map<String, Integer> res = new HashMap<String, Integer>();

    if (m1 != null) {
      for (Map.Entry<String, Integer> entry : m1.entrySet()) {
        res.put(entry.getKey(), entry.getValue());
      }
    }

    if (m2 != null) {
      for (Map.Entry<String, Integer> entry : m2.entrySet()) {
        res.put(entry.getKey(), entry.getValue());
      }
    }

    return res;
  }

  public @DataTypeHint("MULTISET<STRING>") Map<String, Integer> eval(
      @DataTypeHint("MULTISET<STRING>") Map<String, Integer> m1, String s) {

    if (m1 == null && s == null) {
      return null;
    }

    Map<String, Integer> res = new HashMap<String, Integer>();

    if (m1 != null) {
      for (Map.Entry<String, Integer> entry : m1.entrySet()) {
        res.put(entry.getKey(), entry.getValue());
      }
    }

    if (s != null) {
      res.put(s, 1);
    }

    return res;
  }

  public @DataTypeHint("MULTISET<STRING>") Map<String, Integer> eval(String... inputs) {
    Map<String, Integer> res = new HashMap<String, Integer>();

    for (String s : inputs) {
      res.put(s, 1);
    }

    return res;
  }

}
