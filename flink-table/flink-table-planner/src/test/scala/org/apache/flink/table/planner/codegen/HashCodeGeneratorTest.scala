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
package org.apache.flink.table.planner.codegen

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.data.{GenericArrayData, GenericMapData, GenericRowData, StringData}
import org.apache.flink.table.types.logical.{ArrayType, BigIntType, IntType, MapType, MultisetType, RowType, VarBinaryType, VarCharType}

import org.junit.{Assert, Rule, Test}
import org.junit.rules.ExpectedException

import scala.collection.JavaConversions.mapAsJavaMap

/** Test for [[HashCodeGenerator]]. */
class HashCodeGeneratorTest {

  private val classLoader = Thread.currentThread().getContextClassLoader

  var expectedException: ExpectedException = ExpectedException.none

  @Rule
  def thrown: ExpectedException = expectedException

  @Test
  def testRowHash(): Unit = {
    val hashFunc1 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, Thread.currentThread().getContextClassLoader),
        RowType.of(new IntType(), new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)),
        "name",
        Array(1, 0)
      )
      .newInstance(classLoader)

    val hashFunc2 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, Thread.currentThread().getContextClassLoader),
        RowType.of(new IntType(), new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)),
        "name",
        Array(1, 2, 0)
      )
      .newInstance(classLoader)

    val row = GenericRowData.of(ji(5), jl(8), Array[Byte](1, 5, 6))
    Assert.assertEquals(637, hashFunc1.hashCode(row))
    Assert.assertEquals(136516167, hashFunc2.hashCode(row))

    // test row with nested array and map type
    val hashFunc3 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration),
        RowType.of(
          new IntType(),
          new ArrayType(new IntType()),
          new MultisetType(new IntType()),
          new MapType(new IntType(), new VarCharType())),
        "name",
        Array(1, 2, 0, 3)
      )
      .newInstance(classLoader)

    val row3 = GenericRowData.of(
      ji(5),
      new GenericArrayData(Array(1, 5, 7)),
      new GenericMapData(Map(1 -> null, 5 -> null, 10 -> null)),
      new GenericMapData(
        Map(1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny"), 10 -> null))
    )
    Assert.assertEquals(368915957, hashFunc3.hashCode(row3))

    // test hash code for ArrayData
    thrown.expect(classOf[RuntimeException])
    thrown.expectMessage(
      "RowData hash function doesn't support to generate hash code for ArrayData.")
    hashFunc3.hashCode(new GenericArrayData(Array(1)))

    // test hash code for MapData
    thrown.expect(classOf[RuntimeException])
    thrown.expectMessage(
      "RowData hash function doesn't support to generate hash code for ArrayData.")
    hashFunc3.hashCode(new GenericMapData(null))
  }

  @Test
  def testArrayHash(): Unit = {
    // test primitive type
    val hashFunc1 = HashCodeGenerator
      .generateArrayHash(new CodeGeneratorContext(new Configuration()), new IntType(), "name")
      .newInstance(classLoader)

    val array1 = new GenericArrayData(Array(1, 5, 7))
    Assert.assertEquals(13, hashFunc1.hashCode(array1))

    // test complex map type of element
    val hashFunc2 = HashCodeGenerator
      .generateArrayHash(
        new CodeGeneratorContext(new Configuration()),
        new MapType(new IntType(), new VarCharType()),
        "name")
      .newInstance(classLoader)

    val mapData = new GenericMapData(
      Map(1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny"), 10 -> null))
    val array2 = new GenericArrayData(Array[AnyRef](mapData))
    Assert.assertEquals(357483069, hashFunc2.hashCode(array2))

    // test complex row type of element
    val hashFunc3 = HashCodeGenerator
      .generateArrayHash(
        new CodeGeneratorContext(new Configuration()),
        RowType.of(new IntType(), new BigIntType()),
        "name")
      .newInstance(classLoader)

    val array3 = new GenericArrayData(
      Array[AnyRef](GenericRowData.of(ji(5), jl(8)), GenericRowData.of(ji(25), jl(52))))
    Assert.assertEquals(2430, hashFunc3.hashCode(array3))

    // test hash code for RowData
    thrown.expect(classOf[RuntimeException])
    thrown.expectMessage(
      "ArrayData hash function doesn't support to generate hash code for RowData.")
    hashFunc3.hashCode(GenericRowData.of(null))

    // test hash code for MapData
    thrown.expect(classOf[RuntimeException])
    thrown.expectMessage(
      "ArrayData hash function doesn't support to generate hash code for RowData.")
    hashFunc3.hashCode(new GenericMapData(null))
  }

  @Test
  def testMapHash(): Unit = {
    // test primitive type
    val hashFunc1 = HashCodeGenerator
      .generateMapHash(
        new CodeGeneratorContext(new Configuration()),
        new IntType(),
        new VarCharType(),
        "name")
      .newInstance(classLoader)

    val map1 = new GenericMapData(
      Map(1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny"), 10 -> null))
    Assert.assertEquals(357483069, hashFunc1.hashCode(map1))

    // test complex row type of value
    val hashFunc2 = HashCodeGenerator
      .generateMapHash(
        new CodeGeneratorContext(new Configuration()),
        new IntType(),
        RowType.of(new IntType(), new BigIntType()),
        "name")
      .newInstance(classLoader)

    val map2 = new GenericMapData(
      Map(1 -> GenericRowData.of(ji(5), jl(8)), 5 -> GenericRowData.of(ji(54), jl(78)), 10 -> null))
    Assert.assertEquals(4763, hashFunc2.hashCode(map2))

    // test complex array type of value
    val hashFunc3 = HashCodeGenerator
      .generateMapHash(
        new CodeGeneratorContext(new Configuration()),
        new IntType(),
        new ArrayType(new IntType()),
        "name")
      .newInstance(classLoader)

    val map3 = new GenericMapData(
      Map(
        1 -> new GenericArrayData(Array(1, 5, 7)),
        5 -> new GenericArrayData(Array(2, 4, 8)),
        10 -> null))
    Assert.assertEquals(43, hashFunc3.hashCode(map3))

    // test hash code for RowData
    thrown.expect(classOf[RuntimeException])
    thrown.expectMessage("MapData hash function doesn't support to generate hash code for RowData.")
    hashFunc3.hashCode(GenericRowData.of(null))

    // test hash code for ArrayData
    thrown.expect(classOf[RuntimeException])
    thrown.expectMessage(
      "MapData hash function doesn't support to generate hash code for ArrayData.")
    hashFunc3.hashCode(new GenericArrayData(Array(1)))
  }

  def ji(i: Int): Integer = {
    new Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }
}
