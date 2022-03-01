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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._


/**
  * A FlinkOptimizeProgram contains a sequence of [[FlinkOptimizeProgram]]s which are chained
  * together.
  *
  * The chained-order of programs can be adjusted by [[addFirst]], [[addLast]], [[addBefore]]
  * and [[remove]] methods.
  *
  * When [[optimize]] method called, each program's optimize method will be called in sequence.
  *
  * @tparam OC OptimizeContext
  */
class FlinkChainedProgram[OC <: FlinkOptimizeContext]
  extends FlinkOptimizeProgram[OC]
  with Logging {

  // keep program as ordered
  private val programNames = new util.ArrayList[String]()
  // map program name to program instance
  private val programMap = new util.HashMap[String, FlinkOptimizeProgram[OC]]()

  /**
    * Calling each program's optimize method in sequence.
    */
  def optimize(root: RelNode, context: OC): RelNode = {
    programNames.foldLeft(root) {
      (input, name) =>
        val program = get(name).getOrElse(throw new TableException(s"This should not happen."))

        val start = System.currentTimeMillis()
        val result = program.optimize(input, context)
        val end = System.currentTimeMillis()

        if (LOG.isDebugEnabled) {
          LOG.debug(s"optimize $name cost ${end - start} ms.\n" +
            s"optimize result: \n${FlinkRelOptUtil.toString(result)}")
        }

        result
    }
  }

  /**
    * Gets program associated with the given name. If not found, return [[None]].
    */
  def get(name: String): Option[FlinkOptimizeProgram[OC]] = Option.apply(programMap.get(name))

  /**
    * Gets FlinkRuleSetProgram associated with the given name. If the program is not found or is
    * not a [[FlinkRuleSetProgram]], return [[None]].
    * This method is mainly used for updating rules in FlinkRuleSetProgram for existed
    * FlinkChainedPrograms instance.
    */
  def getFlinkRuleSetProgram(name: String): Option[FlinkRuleSetProgram[OC]] = {
    get(name).getOrElse(None) match {
      case p: FlinkRuleSetProgram[OC] => Some(p)
      case _ => None
    }
  }

  /**
    * Appends the specified program to the end of program collection.
    *
    * @return false if program collection contains the specified program; otherwise true.
    */
  def addLast(name: String, program: FlinkOptimizeProgram[OC]): Boolean = {
    Preconditions.checkNotNull(name)
    Preconditions.checkNotNull(program)

    if (programNames.contains(name)) {
      false
    } else {
      programNames.add(name)
      programMap.put(name, program)
      true
    }
  }

  /**
    * Inserts the specified program to the beginning of program collection.
    *
    * @return false if program collection contains the specified program; otherwise true.
    */
  def addFirst(name: String, program: FlinkOptimizeProgram[OC]): Boolean = {
    Preconditions.checkNotNull(name)
    Preconditions.checkNotNull(program)

    if (programNames.contains(name)) {
      false
    } else {
      programNames.add(0, name)
      programMap.put(name, program)
      true
    }
  }

  /**
    * Inserts the specified program before `nameOfBefore`.
    *
    * @return false if program collection contains the specified program or
    *         does not contain `nameOfBefore`; otherwise true.
    */
  def addBefore(nameOfBefore: String, name: String, program: FlinkOptimizeProgram[OC]): Boolean = {
    Preconditions.checkNotNull(nameOfBefore)
    Preconditions.checkNotNull(name)
    Preconditions.checkNotNull(program)

    if (programNames.contains(name) || !programNames.contains(nameOfBefore)) {
      false
    } else if (programNames.isEmpty) {
      addLast(name, program)
    } else {
      val index = programNames.indexOf(nameOfBefore)
      programNames.add(index, name)
      programMap.put(name, program)
      true
    }
  }

  /**
    * Removes program associated with the given name from program collection.
    *
    * @return The removed program associated with the given name. If not found, return [[None]].
    */
  def remove(name: String): Option[FlinkOptimizeProgram[OC]] = {
    programNames.remove(name)
    Option.apply(programMap.remove(name))
  }

  /**
    * Returns program names with chained order.
    */
  def getProgramNames: util.List[String] = new util.ArrayList[String](programNames)

}
