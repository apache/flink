package test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource


/**
  * Created by zhuoluo.yzl on 2017/2/16.
  */
object TableApp {

  def main(args: Array[String]): Unit = {
    what("Hi")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tabEnv = TableEnvironment.getTableEnvironment(env)
    val csvTableSource = new CsvTableSource(
      "/Users/zhuoluo.yzl/dbgen100m/customer.tbl",
      Array("custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"),
      Array(Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.DECIMAL, Types.STRING, Types.STRING),
      fieldDelim = "|")

    tabEnv.registerTableSource("customer", csvTableSource)
    tabEnv.registerHiveUDF("zl_len", "test.TestHiveUdf")
    val table = tabEnv.scan("customer").select("zl_len(name), nationkey, phone")
    val sink = new CsvTableSink("/Users/zhuoluo.yzl/customer", ",")

    table.writeToSink(sink)
    env.execute()

  }

  def what(args: String*): Unit = {
    println(args.length)
  }
}
