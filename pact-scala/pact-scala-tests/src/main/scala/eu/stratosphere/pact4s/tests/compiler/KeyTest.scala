/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.pact4s.tests.compiler

import eu.stratosphere.pact4s.common.analysis._

class KeyTest {

  def toKS[T1: UDT, R](fun: FieldSelector[T1 => R]) = fun

  abstract sealed class TestBase { val baseField: String }
  case class TestSub1(baseField: String, subField: Int) extends TestBase
  case class TestSub2(baseField: String, subField: Double) extends TestBase

  object SelectTests {
    val testID = toKS[(Int, Int, Int), (Int, Int, Int)] { x => x }
    val testAtomic = toKS[(Int, Int, Int), Int] { x => x._2 }
    val testComposite = toKS[(Int, Int, Int), (Int, Int)] { x => (x._3, x._1) }
    val testAtomicComplex = toKS[((Int, Int), (Int, Int)), (Int, Int)] { x => x._2 }
    val testCompositeComplex = toKS[((Int, Int), (Int, Int)), ((Int, Int), (Int, Int))] { x => (x._2, x._1) }
    val testCompositeComplexDeep = toKS[((Int, Int), (Int, Int)), (Int, (Int, Int))] { x => (x._1._2, x._2) }
    val testBase = toKS[TestBase, String] { x => x.baseField }
  }

  object MatchTests {
    val testID = toKS[(Int, Int, Int), (Int, Int, Int)] { case x => x }
    val testAtomic = toKS[(Int, Int, Int), Int] { case (_, b, _) => b }
    val testComposite = toKS[(Int, Int, Int), (Int, Int)] { case (a, _, c) => (c, a) }
    val testAtomicComplex = toKS[((Int, Int), (Int, Int)), (Int, Int)] { case ((a, b), c @ (d, e)) => c }
    val testCompositeComplex = toKS[((Int, Int), (Int, Int)), ((Int, Int), (Int, Int))] { case (a, b) => (b, a) }
    val testCompositeComplexDeepPat = toKS[((Int, Int), (Int, Int)), (Int, (Int, Int))] { case ((_, b), c) => (b, c) }
    val testCompositeComplexDeepMix = toKS[((Int, Int), (Int, Int)), (Int, (Int, Int))] { case (ab, c) => (ab._2, c) }
    val testBase = toKS[TestBase, String] { case x => x.baseField }
  }
} 

