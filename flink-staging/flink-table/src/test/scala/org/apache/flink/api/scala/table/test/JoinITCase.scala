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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.table.{Row, ExpressionException}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.AbstractMultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class JoinITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = ""
  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after: Unit = {
    compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  def testJoin: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinDs = ds1.join(ds2).where('b === 'e).select('c, 'g)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
  }

  @Test
  def testJoinWithFilter: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinDs = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n"
  }

  @Test
  def testJoinWithMultipleKeys: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinDs = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
  }

  @Test(expected = classOf[ExpressionException])
  def testJoinNonExistingKey: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinDs = ds1.join(ds2).where('foo === 'e).select('c, 'g)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test(expected = classOf[ExpressionException])
  def testJoinWithNonMatchingKeyTypes: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinDs = ds1.join(ds2).where('a === 'g).select('c, 'g)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test(expected = classOf[ExpressionException])
  def testJoinWithAmbiguousFields: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'c)

    val joinDs = ds1.join(ds2).where('a === 'd).select('c, 'g)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test
  def testJoinWithAggregation: Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinDs = ds1.join(ds2).where('a === 'd).select('g.count)

    joinDs.toSet[Row].writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "6"
  }


}
