package org.apache.flink.table.executor

import org.apache.flink.api.common.InputDependencyConstraint
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData.{INT_DOUBLE, data2_1, data2_2, data2_3, data3, data5, nullData3, nullData5, nullablesOfData2_3, nullablesOfData3, nullablesOfData5, nullablesOfNullData3, nullablesOfNullData5, nullablesOfSmallData3, smallData3, type3, type5}

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

import scala.collection.Seq

class BatchExecutorExecTest extends BatchTestBase {
  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("NullTable3", nullData3, type3, "a, b, c", nullablesOfNullData3)
    registerCollection("NullTable5", nullData5, type5, "d, e, f, g, h", nullablesOfNullData5)
    registerCollection("l", data2_1, INT_DOUBLE, "a, b")
    registerCollection("r", data2_2, INT_DOUBLE, "c, d")
    registerCollection("t", data2_3, INT_DOUBLE, "c, d", nullablesOfData2_3)
  }

  @Test
  def testJoin(): Unit = {
    env.setBufferTimeout(11)
    env.getConfig.disableObjectReuse()
    env.getConfig.setLatencyTrackingInterval(100)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ANY)
    env.getCheckpointConfig.setCheckpointInterval(5000)
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
    assertEquals(11, env.getBufferTimeout)
    assertTrue(!env.getConfig.isObjectReuseEnabled)
    assertEquals(100, env.getConfig.getLatencyTrackingInterval)
    assertEquals(TimeCharacteristic.EventTime, env.getStreamTimeCharacteristic)
    assertEquals(InputDependencyConstraint.ANY,
      env.getConfig.getDefaultInputDependencyConstraint)
    assertEquals(5000, env.getCheckpointConfig.getCheckpointInterval)
  }
}
