package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

@RunWith(classOf[Parameterized])
class TimeAttributesITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testMissingTimeAttributeThrowsCorrectException(): Unit = {
    val data = List(1L -> "hello", 2L -> "world")
    val stream = env.fromCollection[(Long, String)](data)

    tEnv.createTemporaryView("test", stream, $"event_time".rowtime(), $"data")
    val result = tEnv.sqlQuery("SELECT * FROM test")

    val sink = new TestingAppendSink()
    tEnv.toAppendStream[Row](result).addSink(sink)
    try {
      env.execute()
    } catch {
      case je: JobExecutionException =>
        val innerCause = je.getCause.getCause
        assert(innerCause.isInstanceOf[RuntimeException])
        assertEquals(
          "Rowtime timestamp is not defined. Please make sure that a " +
            "proper TimestampAssigner is defined and the stream environment uses the EventTime " +
            "time characteristic.",
          innerCause.getMessage)
      case e: Exception => fail(s"Expected JobExecutionException, received $e")
    }
  }

  @Test
  def testTimestampAttributesWithWatermarkStrategy(): Unit = {
    val data = List(Instant.now().toEpochMilli -> "hello", Instant.now().toEpochMilli -> "world")
    val stream = env.fromCollection[(Long, String)](data).assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[(Long, String)](Duration.ofMinutes(5))
        .withTimestampAssigner {
          new SerializableTimestampAssigner[(Long, String)] {
            override def extractTimestamp(element: (Long, String), recordTimestamp: Long): Long =
              element._1
          }
        }
    )

    tEnv.createTemporaryView("test", stream, $"event_time".rowtime(), $"data")
    val result = tEnv.sqlQuery("SELECT * FROM test")

    val sink = new TestingAppendSink()
    tEnv.toAppendStream[Row](result).addSink(sink)
    env.execute()

    val formattedData = data.map {
      case (timestamp, data) =>
        val formattedTimestamp =
          LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC).toString
        s"$formattedTimestamp,$data"
    }
    assertEquals(sink.getAppendResults.sorted, formattedData.sorted)
  }
}
