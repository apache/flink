package test

import org.apache.hadoop.hive.ql.exec.UDF

/**
  * Created by zhuoluo.yzl on 2017/2/17.
  */
class TestHiveUdf extends UDF{

  def evaluate(s: String): Int = {
    s.length
  }
}
