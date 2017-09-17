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

package org.apache.flink.table.plan.util

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions._
import org.apache.flink.table.sources.Partition
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionPrunerTest {

  val partitionFieldNames = Array("part1", "part2")
  val partitionFieldTypes: Array[TypeInformation[_]] =
    Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
  val allPartitions = Seq(
    new TestPartition("part1=p1,part2=2").asInstanceOf[Partition],
    new TestPartition("part1=p3,part2=4").asInstanceOf[Partition],
    new TestPartition("part1=p5,part2=6").asInstanceOf[Partition],
    new TestPartition("part1=p7,part2=8").asInstanceOf[Partition]).toList.asJava
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env)
  val relBuilder = tEnv.getRelBuilder

  def getRemainingPartitions(
    allPartitions: JList[Partition],
    partitionPredicate: Array[Expression]): JList[Partition] = {
    PartitionPruner.INSTANCE.getRemainingPartitions(
      partitionFieldNames,
      partitionFieldTypes,
      allPartitions,
      partitionPredicate,
      relBuilder)
  }

  def resolveFields(predicate: Array[Expression]): Array[Expression] = {
    val fieldTypes = partitionFieldNames.zip(partitionFieldTypes).toMap
    val rule: PartialFunction[Expression, Expression] = {
      case u@UnresolvedFieldReference(name) =>
        ResolvedFieldReference(name, fieldTypes(name))
    }
    predicate.map(_.postOrderTransform(rule))
  }

  @Test
  def testEmptyPartitions(): Unit = {
    val allPartitions = new JArrayList[Partition]()
    val predicate = Array[Expression]('part1 === "p1")
    val remainingPartitions = getRemainingPartitions(allPartitions, predicate)
    assertTrue(remainingPartitions.isEmpty)
  }

  @Test
  def testOnePartition(): Unit = {
    val predicate1 = resolveFields(Array[Expression]('part1 === "p1"))
    val remainingPartitions1 = getRemainingPartitions(allPartitions, predicate1)
    assertEquals(1, remainingPartitions1.size())
    assertEquals("part1=p1,part2=2", remainingPartitions1.get(0).getOriginValue)

    val predicate2 = resolveFields(Array[Expression]('part2 === 4))
    val remainingPartitions2 = getRemainingPartitions(allPartitions, predicate2)
    assertEquals(1, remainingPartitions2.size())
    assertEquals("part1=p3,part2=4", remainingPartitions2.get(0).getOriginValue)
  }

  @Test
  def testTwoPartitionAnd(): Unit = {
    val predicate = resolveFields(Array[Expression]('part1 === "p3", 'part2 === 4))
    val remainingPartitions = getRemainingPartitions(allPartitions, predicate)
    assertEquals(1, remainingPartitions.size())
    assertEquals("part1=p3,part2=4", remainingPartitions.get(0).getOriginValue)
  }

  @Test
  def testTwoPartitionOr(): Unit = {
    val predicate = resolveFields(Array[Expression]('part1 === "p1" || 'part2 === 4))
    val remainingPartitions = getRemainingPartitions(allPartitions, predicate)
    assertEquals(2, remainingPartitions.size())
    assertEquals("part1=p1,part2=2", remainingPartitions.get(0).getOriginValue)
    assertEquals("part1=p3,part2=4", remainingPartitions.get(1).getOriginValue)
  }

}

class TestPartition(partition: String) extends Partition {

  private val map = mutable.Map[String, Any]()
  partition.split(",").foreach { p =>
    val kv = p.split("=")
    map.put(kv(0), kv(1))
  }

  override def getFieldValue(fieldName: String): Any = map.getOrElse(fieldName, null)

  override def getOriginValue: String = partition
}
