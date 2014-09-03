/**
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

package org.apache.flink.api.scala

import org.junit.Test
import org.apache.flink.api.common.InvalidProgramException

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators._
import org.scalatest.junit.AssertionsForJUnit

// Verify that the sanity checking in delta iterations works. We just
// have a dummy job that is not meant to be executed. Only verify that
// the join/coGroup inside the iteration is checked.
class DeltaIterationSanityCheckTest extends Serializable {

  @Test
  def testCorrectJoinWithSolution1 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = s join ws where {_._1} isEqualTo {_._1} map { (l, r) => l }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test
  def testCorrectJoinWithSolution2 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = ws join s where {_._1} isEqualTo {_._1} map { (l, r) => l }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectJoinWithSolution1 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = s join ws where {_._2} isEqualTo {_._2} map { (l, r) => l }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectJoinWithSolution2 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = ws join s where {_._2} isEqualTo {_._2} map { (l, r) => l }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectJoinWithSolution3 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = s join ws where {_._1} isEqualTo {_._1} map { (l, r) => l }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._2}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
   }

  @Test
  def testCorrectCoGroupWithSolution1 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = s cogroup ws where {_._1} isEqualTo {_._1} map { (l, r) => l.next() }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test
  def testCorrectCoGroupWithSolution2 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = ws cogroup s where {_._1} isEqualTo {_._1} map { (l, r) => l.next() }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectCoGroupWithSolution1 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = s cogroup ws where {_._2} isEqualTo {_._2} map { (l, r) => l.next() }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectCoGroupWithSolution2 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = ws cogroup s where {_._2} isEqualTo {_._2} map { (l, r) => l.next() }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._1}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectCoGroupWithSolution3 {
    val solutionInput = CollectionDataSource(Array((1, "1")))
    val worksetInput = CollectionDataSource(Array((2, "2")))

    def step(s: DataSet[(Int, String)], ws: DataSet[(Int, String)]) = {
      val result = s cogroup ws where {_._1} isEqualTo {_._1} map { (l, r) => l.next() }
      (result, ws)
    }
    val iteration = solutionInput.iterateWithDelta(worksetInput, {_._2}, step, 10)

    val output = iteration.write("/dummy", CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output))
  }
}
