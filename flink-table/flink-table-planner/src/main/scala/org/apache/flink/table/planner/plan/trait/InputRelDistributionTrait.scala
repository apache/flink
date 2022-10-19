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
package org.apache.flink.table.planner.plan.`trait`

import org.apache.flink.table.planner.plan.`trait`.InputRelDistributionTrait.getOneDistributionKeys
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamRemoveRedundantExchangeRule
import org.apache.flink.util.Preconditions

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.rel.{RelDistribution, RelFieldCollation}
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.mapping.Mappings

import java.util

import scala.Int.box
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * Description of the physical distribution of a relational expression inputs. This trait is used in
 * [[StreamRemoveRedundantExchangeRule]].
 *
 * NOTE: currently only hash input distribution is supported.
 */
class InputRelDistributionTrait private (
    distributionType: RelDistribution.Type,
    // RelNodes with 2 inputs can have 2 key sets
    private val inputsKeys: (Option[ImmutableIntList], Option[ImmutableIntList]),
    fieldCollations: Option[ImmutableList[RelFieldCollation]] = None,
    requireStrict: Boolean = true)
  extends FlinkRelDistribution(
    distributionType,
    getOneDistributionKeys(inputsKeys),
    fieldCollations,
    requireStrict) {

  override def getTraitDef: InputRelDistributionTraitDef = InputRelDistributionTraitDef.INSTANCE

  override def satisfies(relTrait: RelTrait): Boolean = relTrait match {
    case other: FlinkRelDistribution =>
      if (distributionType == other.getType && distributionType == Type.HASH_DISTRIBUTED) {
        getLeftKeys.nonEmpty && other.getKeys.equals(getLeftKeys.get) ||
        getRightKeys.nonEmpty && other.getKeys.equals(getRightKeys.get)
      } else {
        false
      }
    case _ => false
  }

  override def apply(mapping: Mappings.TargetMapping): InputRelDistributionTrait = {
    if (distributionType == Type.HASH_DISTRIBUTED) {
      def keysMapFunc(keys: ImmutableIntList): Option[util.List[Integer]] = {
        val newKeys = keys.map(
          key => {
            try {
              mapping.getTargetOpt(key)
            } catch {
              case _: IndexOutOfBoundsException => -1
            }
          })
        if (newKeys.exists(key => key < 0)) {
          None
        } else {
          Some(newKeys.map(box).toList.asJava)
        }
      }
      val newLeftKeys = inputsKeys._1.flatMap(keysMapFunc)
      val newRightKeys = inputsKeys._2.flatMap(keysMapFunc)
      if (newLeftKeys.nonEmpty && newRightKeys.nonEmpty) {
        InputRelDistributionTrait.twoInputsHash(newLeftKeys.get, newRightKeys.get)
      } else if (newLeftKeys.nonEmpty) {
        InputRelDistributionTrait.leftInputHash(newLeftKeys.get)
      } else if (newRightKeys.nonEmpty) {
        InputRelDistributionTrait.rightInputHash(newRightKeys.get)
      } else {
        InputRelDistributionTrait.ANY
      }
    } else {
      this
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: InputRelDistributionTrait =>
      distributionType == that.getType &&
      inputsKeys == that.inputsKeys && fieldCollations == that.getFieldCollations &&
      requireStrict == that.requireStrict
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(distributionType, inputsKeys, fieldCollations, requireStrict)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def getLeftKeys: Option[ImmutableIntList] = {
    inputsKeys._1
  }

  def getRightKeys: Option[ImmutableIntList] = {
    inputsKeys._2
  }
}

object InputRelDistributionTrait {

  val ANY =
    new InputRelDistributionTrait(RelDistribution.Type.ANY, (Some(ImmutableIntList.of), None))

  def inputHash(
      columns: util.Collection[_ <: Number],
      requireStrict: Boolean = true
  ): InputRelDistributionTrait = {
    leftInputHash(columns, requireStrict)
  }

  def leftInputHash(
      columns: util.Collection[_ <: Number],
      requireStrict: Boolean = true
  ): InputRelDistributionTrait = {
    val list = ImmutableIntList.copyOf(columns)
    canonize(
      new InputRelDistributionTrait(
        Type.HASH_DISTRIBUTED,
        (Some(list), None),
        requireStrict = requireStrict))
  }

  def rightInputHash(
      columns: util.Collection[_ <: Number],
      requireStrict: Boolean = true
  ): InputRelDistributionTrait = {
    val list = ImmutableIntList.copyOf(columns)
    canonize(
      new InputRelDistributionTrait(
        Type.HASH_DISTRIBUTED,
        (None, Some(list)),
        requireStrict = requireStrict))
  }

  def twoInputsHash(
      columns1: util.Collection[_ <: Number],
      columns2: util.Collection[_ <: Number],
      requireStrict: Boolean = true
  ): InputRelDistributionTrait = {
    val list1 = ImmutableIntList.copyOf(columns1)
    val list2 = ImmutableIntList.copyOf(columns2)
    canonize(
      new InputRelDistributionTrait(
        Type.HASH_DISTRIBUTED,
        (Some(list1), Some(list2)),
        requireStrict = requireStrict))
  }

  private def getOneDistributionKeys(
      inputsKeys: (Option[ImmutableIntList], Option[ImmutableIntList])): ImmutableIntList = {
    Preconditions.checkState(inputsKeys._1.nonEmpty || inputsKeys._2.nonEmpty)
    if (inputsKeys._1.nonEmpty) {
      inputsKeys._1.get
    } else {
      inputsKeys._2.get
    }
  }

  private def canonize(in: InputRelDistributionTrait): InputRelDistributionTrait = {
    InputRelDistributionTraitDef.INSTANCE.canonize(in)
  }
}
