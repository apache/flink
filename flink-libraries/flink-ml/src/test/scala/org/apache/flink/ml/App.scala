package org.apache.flink.ml

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._

/**
  * Test
  */
object App {


  def main(args: Array[String]) {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val fitData = env.fromCollection(List("a","b","c","a","a","d","a","a","a","b","b","c","a","c","b","c","b"))

    fitData.map( s => (s, 1) )
      .groupBy( 0 )
      .sum(1)
      .partitionByRange( x => - x._2 )
      .sortPartition(1, Order.DESCENDING)
      .zipWithIndex
      .print()

  }
}
