package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;

/* CREATE FUNCTION [IF NOT EXISTS] [catalog_name.][db_name.]function_name
  * AS class_name [LANGUAGE JAVA|SCALA]
  * 
  * create function TestFunc as 'com.fentik.TestFunc' language java;
  */

public class TestFunc extends ScalarFunction {

  public String eval(String input) {
    // test identity function
    return "got input:" + input;
  }
}