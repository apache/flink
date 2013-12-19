/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.scala

import scala.collection.TraversableOnce
import eu.stratosphere.api.scala.analysis._
import eu.stratosphere.api.common.operators.Operator

package object operators {


//  implicit def funToRepeat[SolutionItem: UDT](stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]) = new RepeatOperator(stepFunction)
//  implicit def funToIterate[SolutionItem, DeltaItem](s0: DataStream[SolutionItem]) = new IterateOperator(s0.contract)
//  implicit def funToWorksetIterate[SolutionItem: UDT, WorksetItem: UDT](stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) = new WorksetIterateOperator(stepFunction)

  implicit def traversableToIterator[T](i: TraversableOnce[T]): Iterator[T] = i.toIterator
  implicit def optionToIterator[T](opt: Option[T]): Iterator[T] = opt.iterator
  implicit def arrayToIterator[T](arr: Array[T]): Iterator[T] = arr.iterator
}