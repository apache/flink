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

import language.experimental.macros
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.scala.operators.CoGroupDataSet
import eu.stratosphere.api.scala.operators.CrossDataSet
import eu.stratosphere.api.scala.operators.JoinDataSet
import eu.stratosphere.api.scala.operators.MapMacros
import eu.stratosphere.api.scala.operators.KeyedDataSet
import eu.stratosphere.api.scala.operators.ReduceMacros
import eu.stratosphere.api.scala.operators.UnionMacros
import eu.stratosphere.api.scala.operators.IterateMacros
import eu.stratosphere.api.scala.operators.WorksetIterateMacros

class DataSet[T] (val contract: Operator with ScalaOperator[T]) {
  
  def cogroup[RightIn](rightInput: DataSet[RightIn]) = new CoGroupDataSet[T, RightIn](this, rightInput)
  def cross[RightIn](rightInput: DataSet[RightIn]) = new CrossDataSet[T, RightIn](this, rightInput)
  def join[RightIn](rightInput: DataSet[RightIn]) = new JoinDataSet[T, RightIn](this, rightInput)
  
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
  def iterateWithTermination[C](n: Int, stepFunction: DataSet[T] => DataSet[T], terminationFunction: (DataSet[T],
    DataSet[T]) => DataSet[C]) = macro IterateMacros.iterateWithTermination[T, C]
  def iterateWithDelta[SolutionKey, WorksetItem](workset: DataSet[WorksetItem], solutionSetKey: T => SolutionKey, stepFunction: (DataSet[T], DataSet[WorksetItem]) => (DataSet[T], DataSet[WorksetItem]), maxIterations: Int) = macro WorksetIterateMacros.iterateWithDelta[T, SolutionKey, WorksetItem]
  
  def write(url: String, format: ScalaOutputFormat[T]) = DataSinkOperator.write(this, url, format)
  def write(url: String, format: ScalaOutputFormat[T], name: String) = DataSinkOperator.write(this, url, format, name)
  
}