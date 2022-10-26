/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.bridge.scala
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, StringSink}
import org.apache.flink.table.planner.runtime.utils.JavaPojos.{Device, Order, Person, ProductItem}

import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util.Collections

/** Integration tests for methods on [[StreamTableEnvironment]]. */
class StreamTableEnvironmentITCase extends StreamingTestBase {

  @Test
  def testToAppendStreamWithPojoType(): Unit = {
    val orderA = env.fromCollection(
      Seq(
        new Order(1L, new ProductItem("beer", 10L), 3),
        new Order(1L, new ProductItem("diaper", 11L), 4),
        new Order(3L, new ProductItem("rubber", 12L), 2)))

    val orderB: DataStream[Order] = env.fromCollection(
      Seq(
        new Order(2L, new ProductItem("pen", 13L), 3),
        new Order(2L, new ProductItem("rubber", 12L), 3),
        new Order(4L, new ProductItem("beer", 10L), 1)))

    // convert DataStream to Table
    val tableA = tEnv.fromDataStream(orderA, 'user, 'product, 'amount)
    // register DataStream as Table
    tEnv.createTemporaryView("OrderB", orderB, 'user, 'product, 'amount)

    // union the two tables
    val result = tEnv.sqlQuery(s"""
                                  |SELECT * FROM $tableA WHERE amount > 2
                                  |UNION ALL
                                  |SELECT * FROM OrderB WHERE amount < 2
        """.stripMargin)

    val sink = new StringSink[Order]()
    result.toAppendStream[Order].addSink(sink)

    env.execute()

    val expected = List(
      "Order{user=1, product='Product{name='beer', id=10}', amount=3}",
      "Order{user=1, product='Product{name='diaper', id=11}', amount=4}",
      "Order{user=4, product='Product{name='beer', id=10}', amount=1}"
    )
    assertEquals(expected.sorted, sink.getResults.sorted)
  }

  @Test
  def testToAppendStreamWithRawType(): Unit = {
    val devices = env.fromCollection(
      Seq(
        new Device(1L, "device1", Collections.singletonMap("A", 10)),
        new Device(2L, "device2", Collections.emptyMap()),
        new Device(3L, "device3", Collections.singletonMap("B", 20))
      ))

    // register DataStream as Table
    tEnv.createTemporaryView("devices", devices, 'deviceId, 'deviceName, 'metrics)

    val result = tEnv.sqlQuery("SELECT * FROM devices WHERE deviceId >= 2")
    val sink = new StringSink[Device]()
    result.toAppendStream[Device].addSink(sink)

    env.execute()

    val expected = List(
      "Device{deviceId=2, deviceName='device2', metrics={}}",
      "Device{deviceId=3, deviceName='device3', metrics={B=20}}")
    assertEquals(expected.sorted, sink.getResults.sorted)
  }

  @Test
  def testToRetractStreamWithPojoType(): Unit = {
    val persons = env.fromCollection(
      Seq(
        new Person("bob", 1),
        new Person("Liz", 2),
        new Person("Jack", 3)
      ))

    tEnv.createTemporaryView("person", persons)
    val sink = new StringSink[(Boolean, Person)]()
    // reorder the fields (fields order in PojoTypeInfo is [age, name])
    tEnv.sqlQuery("select name, age from person").toRetractStream[Person].addSink(sink)
    env.execute()

    val expected = List(
      "(true,Person{name='bob', age=1})",
      "(true,Person{name='Liz', age=2})",
      "(true,Person{name='Jack', age=3})")
    assertEquals(expected.sorted, sink.getResults.sorted)
  }

  @Test
  def testRetractMsgWithPojoType(): Unit = {
    val orders = env.fromCollection(
      Seq(
        new Order(1L, new ProductItem("beer", 10L), 1),
        new Order(1L, new ProductItem("beer", 10L), 2)
      ))

    val table = tEnv.fromDataStream(orders, 'user, 'product, 'amount)

    val sink = new StringSink[(Boolean, Order)]()
    tEnv
      .sqlQuery(s"""|SELECT user, product, sum(amount) as amount
                    |FROM $table
                    |GROUP BY user, product
                    |""".stripMargin)
      .toRetractStream[Order]
      .addSink(sink)
    env.execute()

    val expected = List(
      "(true,Order{user=1, product='Product{name='beer', id=10}', amount=1})",
      "(false,Order{user=1, product='Product{name='beer', id=10}', amount=1})",
      "(true,Order{user=1, product='Product{name='beer', id=10}', amount=3})"
    )
    assertEquals(expected.sorted, sink.getResults.sorted)
  }

  @Test
  def testTableConfigInheritsEnvironmentSettings(): Unit = {
    val config = new Configuration
    config.setString(TableConfigOptions.TABLE_CATALOG_NAME, "myCatalog")
    val env = StreamExecutionEnvironment.getExecutionEnvironment(config)
    val tEnv = StreamTableEnvironment.create(env)
    assertThat(tEnv.getConfig.get(TableConfigOptions.TABLE_CATALOG_NAME)).isEqualTo("myCatalog")

    val scalaEnv =
      org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.getExecutionEnvironment
    val scalaTEnv = scala.StreamTableEnvironment.create(
      scalaEnv,
      EnvironmentSettings.newInstance.withConfiguration(config).build)
    assertThat(scalaTEnv.getConfig.get(TableConfigOptions.TABLE_CATALOG_NAME))
      .isEqualTo("myCatalog")
  }
}
