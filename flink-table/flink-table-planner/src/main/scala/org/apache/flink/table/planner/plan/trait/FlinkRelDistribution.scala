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

import org.apache.flink.table.planner.JArrayList
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil

import com.google.common.collect.{ImmutableList, Ordering}
import org.apache.calcite.plan.{RelMultipleTrait, RelOptPlanner, RelTrait}
import org.apache.calcite.rel.{RelDistribution, RelFieldCollation}
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.util.{ImmutableIntList, Util}
import org.apache.calcite.util.mapping.Mappings

import java.util

import scala.collection.JavaConversions._

/**
 * Description of the physical distribution of a relational expression. See [[RelDistribution]] for
 * more details.
 *
 * NOTE: it's intended to have a private constructor for this class.
 */
class FlinkRelDistribution private (
    private val distributionType: RelDistribution.Type,
    private val keys: ImmutableIntList,
    private val fieldCollations: Option[ImmutableList[RelFieldCollation]] = None,
    val requireStrict: Boolean = true)
  extends RelDistribution {

  require(
    (distributionType == Type.HASH_DISTRIBUTED)
      || (distributionType == Type.RANGE_DISTRIBUTED)
      || keys.isEmpty)

  require((distributionType != Type.RANGE_DISTRIBUTED) || fieldCollations.nonEmpty)

  private val ORDERING = Ordering.natural[Integer].lexicographical[Integer]

  def getFieldCollations: Option[ImmutableList[RelFieldCollation]] = fieldCollations

  override def getKeys: ImmutableIntList = keys

  override def getType: RelDistribution.Type = distributionType

  override def getTraitDef: FlinkRelDistributionTraitDef = FlinkRelDistributionTraitDef.INSTANCE

  override def satisfies(relTrait: RelTrait): Boolean = relTrait match {
    case other: FlinkRelDistribution =>
      if (this == other || other.getType == Type.ANY) {
        true
      } else if (distributionType == other.distributionType) {
        if (distributionType == Type.HASH_DISTRIBUTED) {
          if (other.requireStrict) {
            // Join and union require strict satisfy.
            // First: Hash[x] does not satisfy Hash[x, y],
            // See https://issues.apache.org/jira/browse/DRILL-1102 for more details
            // Second: Hash[x, y] does not satisfy Hash[y, x].
            this == other
          } else {
            // Agg does not need require strict satisfy.
            // First: Hash[x] satisfy Hash[x, y]
            // Second: Hash[x, y] satisfy Hash[y, x]
            other.keys.containsAll(keys)
          }
        } else if (distributionType == Type.RANGE_DISTRIBUTED) {
          Util.startsWith(other.fieldCollations.get, fieldCollations.get)
        } else {
          true
        }
      } else if (other.distributionType == Type.RANDOM_DISTRIBUTED) {
        // RANDOM is satisfied by HASH, ROUND-ROBIN, RANDOM, RANGE;
        distributionType == Type.HASH_DISTRIBUTED ||
        distributionType == Type.ROUND_ROBIN_DISTRIBUTED ||
        distributionType == Type.RANGE_DISTRIBUTED
      } else {
        false
      }
    case _ => false
  }

  override def apply(mapping: Mappings.TargetMapping): FlinkRelDistribution = {
    if (distributionType == Type.HASH_DISTRIBUTED) {
      val newKeys = new util.ArrayList[Integer]
      keys.foreach {
        key =>
          try {
            val i = mapping.getTargetOpt(key)
            if (i >= 0) {
              newKeys.add(i)
            } else {
              return FlinkRelDistribution.ANY
            }
          } catch {
            case _: IndexOutOfBoundsException => return FlinkRelDistribution.ANY
          }
      }
      FlinkRelDistribution.hash(newKeys, requireStrict)
    } else if (distributionType == Type.RANGE_DISTRIBUTED) {
      val newFieldCollations = new util.ArrayList[RelFieldCollation]
      fieldCollations.get.foreach {
        fieldCollation =>
          try {
            val i = mapping.getTargetOpt(fieldCollation.getFieldIndex)
            if (i >= 0) {
              newFieldCollations.add(fieldCollation.withFieldIndex(i))
            } else {
              return FlinkRelDistribution.ANY
            }
          } catch {
            case _: IndexOutOfBoundsException => return FlinkRelDistribution.ANY
          }
      }
      FlinkRelDistribution.range(newFieldCollations)
    } else {
      this
    }
  }

  override def register(planner: RelOptPlanner): Unit = {}

  def canEqual(other: Any): Boolean = other.isInstanceOf[FlinkRelDistribution]

  override def equals(other: Any): Boolean = other match {
    case that: FlinkRelDistribution =>
      (that.canEqual(this)) &&
      distributionType == that.distributionType &&
      keys == that.keys && fieldCollations == that.fieldCollations &&
      requireStrict == that.requireStrict
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(distributionType, keys, fieldCollations, requireStrict)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    if (keys.isEmpty) {
      distributionType.shortName
    } else if (fieldCollations.nonEmpty) {
      distributionType.shortName + fieldCollations.get.asList + requireStrict
    } else {
      distributionType.shortName + keys + requireStrict
    }
  }

  override def isTop: Boolean = distributionType == Type.ANY

  // here we only need to define a determinate order between RelMultipleTraits
  override def compareTo(o: RelMultipleTrait): Int = o match {
    case other: FlinkRelDistribution =>
      if (this.equals(other)) {
        0
      } else if (distributionType == other.distributionType) {
        if (distributionType == Type.HASH_DISTRIBUTED) {
          ORDERING.compare(
            Ordering.natural().sortedCopy(keys),
            Ordering.natural().sortedCopy(other.keys))
        } else if (distributionType == Type.RANGE_DISTRIBUTED) {
          val collations1 = fieldCollations.get.asList()
          val collations2 = other.fieldCollations.get.asList()
          for (i <- 0 until math.min(collations1.size(), collations2.size())) {
            val c = collations1.get(i).toString.compareTo(collations2.get(i).toString)
            if (c != 0) {
              return c
            }
          }
          if (collations1.size() == collations2.size()) {
            0
          } else {
            if (collations1.size() > collations2.size()) 1 else -1
          }
        } else {
          0
        }
      } else {
        distributionType.compareTo(other.getType)
      }
    case _ => -1
  }
}

object FlinkRelDistribution {

  private val EMPTY: ImmutableIntList = ImmutableIntList.of

  val ANY = new FlinkRelDistribution(RelDistribution.Type.ANY, EMPTY)

  val DEFAULT: FlinkRelDistribution = ANY

  /** The singleton singleton distribution. */
  val SINGLETON = new FlinkRelDistribution(RelDistribution.Type.SINGLETON, EMPTY)

  /** The singleton broadcast distribution. */
  val BROADCAST_DISTRIBUTED =
    new FlinkRelDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED, EMPTY)

  /** The singleton random distribution. */
  val RANDOM_DISTRIBUTED: RelDistribution =
    new FlinkRelDistribution(RelDistribution.Type.RANDOM_DISTRIBUTED, EMPTY)

  /** The singleton round-robin distribution. */
  val ROUND_ROBIN_DISTRIBUTED: RelDistribution =
    new FlinkRelDistribution(RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED, EMPTY)

  def hash(
      columns: util.Collection[_ <: Number],
      requireStrict: Boolean = true): FlinkRelDistribution = {
    val list = ImmutableIntList.copyOf(columns)
    canonize(new FlinkRelDistribution(Type.HASH_DISTRIBUTED, list, requireStrict = requireStrict))
  }

  def hash(columns: Array[Int], requireStrict: Boolean): FlinkRelDistribution = {
    val fields = new JArrayList[Integer]()
    columns.foreach(fields.add(_))
    hash(fields, requireStrict)
  }

  /** Creates a range distribution. */
  def range(collations: util.List[RelFieldCollation]): FlinkRelDistribution = {
    val keys = collations.map(i => Integer.valueOf(i.getFieldIndex)).toList
    val fieldCollations = ImmutableList.copyOf[RelFieldCollation](collations)
    canonize(
      new FlinkRelDistribution(
        RelDistribution.Type.RANGE_DISTRIBUTED,
        ImmutableIntList.copyOf(keys: _*),
        Some(fieldCollations)))
  }

  def range(collations: RelFieldCollation*): FlinkRelDistribution = range(collations)

  def range(columns: util.Collection[_ <: Number]): FlinkRelDistribution = {
    val keys = ImmutableIntList.copyOf(columns)
    val collations = new util.ArrayList[RelFieldCollation]()
    columns.foreach(f => collations.add(FlinkRelOptUtil.ofRelFieldCollation(f.intValue())))
    val fieldCollations = ImmutableList.copyOf[RelFieldCollation](collations)
    canonize(
      new FlinkRelDistribution(RelDistribution.Type.RANGE_DISTRIBUTED, keys, Some(fieldCollations)))
  }

  /** NOTE: All creation of FlinkRelDistribution should be canonized */
  private def canonize(in: FlinkRelDistribution): FlinkRelDistribution = {
    FlinkRelDistributionTraitDef.INSTANCE.canonize(in)
  }
}
