package org.apache.flink.table.executor

import org.apache.flink.api.common.InputDependencyConstraint
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.transformations.ShuffleMode
import org.apache.flink.table.api.ExecutionConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

/**
  * Test for streamEnv config save and restore when run batch jobs.
  */
class BatchExecutorTest extends TableTestBase {

  private var util: BatchTableTestUtil = _

  @Before
  def setUp(): Unit = {
    util = batchTestUtil()
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addDataStream[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)
  }

  @Test
  def testRestoreConfig(): Unit = {
    util.getStreamEnv.setBufferTimeout(11)
    util.getStreamEnv.getConfig.disableObjectReuse()
    util.getStreamEnv.getConfig.setLatencyTrackingInterval(100)
    util.getStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    util.getStreamEnv.getConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ANY)
    util.verifyExplain("SELECT * FROM MyTable")
    assertEquals(11, util.getStreamEnv.getBufferTimeout)
    assertTrue(!util.getStreamEnv.getConfig.isObjectReuseEnabled)
    assertEquals(100, util.getStreamEnv.getConfig.getLatencyTrackingInterval)
    assertEquals(TimeCharacteristic.EventTime, util.getStreamEnv.getStreamTimeCharacteristic)
    assertEquals(InputDependencyConstraint.ANY,
      util.getStreamEnv.getConfig.getDefaultInputDependencyConstraint)
  }

  @Test
  def testRestoreConfigWhenBatchShuffleMode(): Unit = {
    util.getTableEnv.getConfig.getConf.setString(ExecutionConfigOptions.SQL_EXEC_SHUFFLE_MODE,
      ShuffleMode.BATCH.toString)
    util.getStreamEnv.setBufferTimeout(11)
    util.getStreamEnv.getConfig.disableObjectReuse()
    util.getStreamEnv.getConfig.setLatencyTrackingInterval(100)
    util.getStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    util.getStreamEnv.getConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ANY)
    util.verifyExplain("SELECT * FROM MyTable")
    assertEquals(11, util.getStreamEnv.getBufferTimeout)
    assertTrue(!util.getStreamEnv.getConfig.isObjectReuseEnabled)
    assertEquals(100, util.getStreamEnv.getConfig.getLatencyTrackingInterval)
    assertEquals(TimeCharacteristic.EventTime, util.getStreamEnv.getStreamTimeCharacteristic)
    assertEquals(InputDependencyConstraint.ANY,
      util.getStreamEnv.getConfig.getDefaultInputDependencyConstraint)
  }
}
