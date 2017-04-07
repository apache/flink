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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}
import org.apache.flink.table.plan.nodes.datastream.AccMode.AccMode

/**
  * Used to store retraction related properties which is used during rule optimization.
  */
class RetractionTrait extends RelTrait {

  /**
    * Defines whether downstream operator need retraction. Please note that needToRetract is
    * different from needRetraction. NeedToRetract is a property particular for each operator,
    * while NeedRetraction is a property for each input. Most of operators have only one input,
    * some operators may have more than one inputs (e.g., join, union), and the property of the
    * NeedRetraction could be different across different inputs of the same operator
    */
  private var needToRetract: Boolean = false

  /**
    * Defines the accumulating mode for a operator. Basically there are two modes for each
    * operator: Accumulating Mode (Acc) and Accumulating and Retracting Mode (AccRetract).
    */
  private var accMode = AccMode.Acc


  def this(needToRetract: Boolean, accMode: AccMode) {
    this()
    this.needToRetract = needToRetract
    this.accMode = accMode
  }

  def getNeedToRetract = needToRetract

  def getAccMode = accMode


  override def register(planner: RelOptPlanner): Unit = { }

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = RetractionTraitDef.INSTANCE

  override def satisfies(`trait`: RelTrait): Boolean = {
    this.equals(`trait`)
  }

  override def toString: String = {
    s"(${needToRetract.toString}, ${accMode.toString})"
  }

}

object RetractionTrait {
  val DEFAULT = new RetractionTrait(false, AccMode.Acc)
}


/**
  * Use this property to indicates how the accumulating works in an operator. In AccMode, all the
  * inputs of an operator will be just simply accumulated to the existing state and generate an
  * accumulating message to the downstream, while in AccRetractMode besides accumulating message
  * the operator may generate or forward an additional retraction message
  */
object AccMode extends Enumeration {
  type AccMode = Value
  val Acc, AccRetract = Value
}


