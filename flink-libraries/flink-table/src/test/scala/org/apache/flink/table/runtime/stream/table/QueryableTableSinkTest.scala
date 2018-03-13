package org.apache.flink.table.runtime.stream.table

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.base.{BooleanSerializer, StringSerializer}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.LegacyKeyedProcessOperator
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.harness.HarnessTestBase
import org.apache.flink.table.sinks.{QueryableStateProcessFunction, RowKeySelector}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.{mock, verify, when}

class QueryableTableSinkTest extends HarnessTestBase {
  @Test
  def testRowSelector(): Unit = {
    val keyTypes = Array(TypeInformation.of(classOf[List[Int]]), TypeInformation.of(classOf[String]))
    val selector = new RowKeySelector(Array(0, 2), new RowTypeInfo(keyTypes:_*))

    val src = Row.of(List(1), "a", "b")
    val key = selector.getKey(JTuple2.of(true, src))

    assertEquals(Row.of(List(1), "b"), key)
  }

  @Test
  def testProcessFunction(): Unit = {
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.milliseconds(2), Time.milliseconds(10))

    val keys = Array("id")
    val keyType = new RowTypeInfo(TypeInformation.of(classOf[String]))
    val fieldNames = Array("id", "is_manager", "name")
    val fieldTypes: Array[TypeInformation[_]] = Array(
      TypeInformation.of(classOf[String]).asInstanceOf[TypeInformation[_]],
      TypeInformation.of(classOf[JBool]).asInstanceOf[TypeInformation[_]],
      TypeInformation.of(classOf[String]).asInstanceOf[TypeInformation[_]])
    val func = new QueryableStateProcessFunction("test", queryConfig, keys, fieldNames, fieldTypes)

    val operator = new LegacyKeyedProcessOperator[Row, JTuple2[JBool, Row], Void](func)

    val testHarness = createHarnessTester(operator,
      new RowKeySelector(Array(0), keyType),
      keyType)

    testHarness.open()


    val stateDesc1 = new ValueStateDescriptor[JBool]("is_manager", TypeInformation.of(classOf[JBool]))
    stateDesc1.initializeSerializerUnlessSet(operator.getExecutionConfig)
    val stateDesc2 = new ValueStateDescriptor[String]("name", TypeInformation.of(classOf[String]))
    stateDesc2.initializeSerializerUnlessSet(operator.getExecutionConfig)
    testHarness.processElement(JTuple2.of(true, Row.of("1", JBool.valueOf(true), "jeff")), 2)
    testHarness.processElement(JTuple2.of(true, Row.of("2", JBool.valueOf(false), "dean")), 6)

    var state1 = testHarness.getState(Row.of("1"), stateDesc1)
    assert(state1.value())
    state1 = testHarness.getState(Row.of("2"), stateDesc1)
    assert(!state1.value())

    var state2 = testHarness.getState(Row.of("1"), stateDesc2)
    assertEquals("jeff", state2.value())
    state2 = testHarness.getState(Row.of("2"), stateDesc2)
    assertEquals("dean", state2.value())


    testHarness.processElement(JTuple2.of(false, Row.of("1", JBool.valueOf(true), "jeff")), 2)
    testHarness.processElement(JTuple2.of(false, Row.of("2", JBool.valueOf(false), "dean")), 6)

    state1 = testHarness.getState(Row.of("1"), stateDesc1)
    assertEquals(null, state1.value())
    state1 = testHarness.getState(Row.of("2"), stateDesc1)
    assertEquals(null, state1.value())

    state2 = testHarness.getState(Row.of("1"), stateDesc2)
    assertEquals(null, state2.value())
    state2 = testHarness.getState(Row.of("2"), stateDesc2)
    assertEquals(null, state2.value())

  }
}
