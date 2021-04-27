package org.apache.flink.cep.scala

import org.apache.flink.cep
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Test

class CEPScalaApiPatternStreamTest {
  /**
    * These tests simply check that use the Scala API  to update the Characteristic of the PatternStream .
    */

  @Test
  def testUpdatePatternStreamCharacteristicByScalaApi(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[(Int, Int)] = env.fromElements()
    val pattern: Pattern[(Int, Int), (Int, Int)] = Pattern.begin[(Int, Int)]("dummy")

    val pStream: PatternStream[(Int, Int)] = CEP.pattern(dummyDataStream, pattern)
    val jStream: cep.PatternStream[(Int, Int)] = pStream.wrappedPatternStream

    assert(pStream.wrappedPatternStream == jStream)

    //change Characteristic use scala api
    val pStream1: PatternStream[(Int, Int)] = pStream.inProcessingTime()
    assert(pStream1.wrappedPatternStream != jStream)

    val pStream2: PatternStream[(Int, Int)] = pStream.inEventTime()
    assert(pStream2.wrappedPatternStream != jStream)

    val pStream3: PatternStream[(Int, Int)] = pStream.sideOutputLateData(new OutputTag[(Int, Int)]("dummy"))
    assert(pStream3.wrappedPatternStream != jStream)

  }
}
