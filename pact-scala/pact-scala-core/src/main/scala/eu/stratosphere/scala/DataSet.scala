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

package eu.stratosphere.scala

import language.experimental.macros
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.scala.operators.CoGroupDataStream
import eu.stratosphere.scala.operators.CrossDataStream
import eu.stratosphere.scala.operators.JoinDataStream
import eu.stratosphere.scala.operators.MapMacros
import eu.stratosphere.scala.operators.GroupByDataStream
import eu.stratosphere.scala.operators.ReduceMacros
import eu.stratosphere.scala.operators.UnionMacros
import eu.stratosphere.scala.operators.IterateMacros
import eu.stratosphere.scala.operators.WorksetIterateMacros

class DataSet[T] (val contract: Contract with ScalaContract[T]) {
  
  def cogroup[RightIn](rightInput: DataSet[RightIn]) = new CoGroupDataStream[T, RightIn](this, rightInput)
  def cross[RightIn](rightInput: DataSet[RightIn]) = new CrossDataStream[T, RightIn](this, rightInput)
  def join[RightIn](rightInput: DataSet[RightIn]) = new JoinDataStream[T, RightIn](this, rightInput)
  
  def map[Out](fun: T => Out) = macro MapMacros.map[T, Out]
  def flatMap[Out](fun: T => Iterator[Out]) = macro MapMacros.flatMap[T, Out]
  def filter(fun: T => Boolean) = macro MapMacros.filter[T]
  
  // reduce
  def groupBy[Key](keyFun: T => Key) = macro ReduceMacros.groupBy[T, Key]

  // reduce without grouping
  def reduce(fun: (T, T) => T) = macro ReduceMacros.globalReduce[T]
  def reduceAll[Out](fun: Iterator[T] => Out) = macro ReduceMacros.globalReduceGroup[T, Out]
  def combinableReduceAll[Out](fun: Iterator[T] => Out) = macro ReduceMacros.combinableGlobalReduceGroup[T]

  def union(secondInput: DataSet[T]) = macro UnionMacros.impl[T]
  
  def iterateWithDelta[DeltaItem](stepFunction: DataSet[T] => (DataSet[T], DataSet[DeltaItem])) = macro IterateMacros.iterateWithDelta[T, DeltaItem]
  def iterate(n: Int, stepFunction: DataSet[T] => DataSet[T])= macro IterateMacros.iterate[T]
  def iterateWithWorkset[SolutionKey, WorksetItem](workset: DataSet[WorksetItem], solutionSetKey: T => SolutionKey, stepFunction: (DataSet[T], DataSet[WorksetItem]) => (DataSet[T], DataSet[WorksetItem]), maxIterations: Int) = macro WorksetIterateMacros.iterateWithWorkset[T, SolutionKey, WorksetItem]
  
  def write(url: String, format: DataSinkFormat[T]) = DataSinkOperator.write(this, url, format) 
  
}