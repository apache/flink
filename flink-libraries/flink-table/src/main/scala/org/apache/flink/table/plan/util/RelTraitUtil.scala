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

package org.apache.flink.table.plan.util

import org.apache.flink.table.plan.`trait`.{AccModeTraitDef, UpdateAsRetractionTraitDef}

import org.apache.calcite.rel.core.Union
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}

import java.io.{PrintWriter, StringWriter, Writer}

import scala.collection.JavaConversions._

object RelTraitUtil {

  /**
    * Converts a relational expression to a string. Each node only contains node name,
    * retraction traits (including UpdateAsRetractionTraitDef and AccModeTraitDef) if exists.
    */
  def explainRetractTraits(rel: RelNode): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    explainRetractTraits(rel, pw, 0)
    pw.close()
    sw.toString
  }

  private def explainRetractTraits(rel: RelNode, writer: Writer, depth: Int): Unit = {
    val className = rel.getClass.getSimpleName
    val retractString = rel.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE).toString
    val accModString = rel.getTraitSet.getTrait(AccModeTraitDef.INSTANCE).toString
    writer
      .append("  " * depth)
      .append(s"$className(retract=[$retractString], accMode=[$accModString])")
      .append("\n")
    rel match {
      case s: SingleRel => explainRetractTraits(s.getInput, writer, depth + 1)
      case b: BiRel =>
        explainRetractTraits(b.getLeft, writer, depth + 1)
        explainRetractTraits(b.getRight, writer, depth + 1)
      case u: Union =>
        u.getInputs.map(explainRetractTraits(_, writer, depth + 1))
      case _ => // do nothing
    }
  }
}
