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

package org.apache.flink.table.plan.optimize

import org.apache.flink.table.util.Logging

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * FlinkChainedPrograms contains a sequence of [[FlinkOptimizeProgram]]s which are chained
  * together.
  *
  * The chained-order of programs can be adjusted by addXXX and [[remove]] methods.
  *
  * When [[optimize]] method called, each program's optimize method will be called in sequence.
  *
  * @tparam OC OptimizeContext
  */
class FlinkChainedPrograms[OC <: OptimizeContext] extends Logging {
  private val programMap = new mutable.HashMap[String, FlinkOptimizeProgram[OC]]()
  private val programNames = new mutable.ListBuffer[String]()

  /**
    * Calling each program's optimize method in sequence.
    */
  def optimize(root: RelNode, context: OC): RelNode = {
    programNames.foldLeft(root) {
      (input, name) =>
        val start = System.currentTimeMillis()
        val result = get(name)
            .getOrElse(throw new RuntimeException(s"program of $name does not exist"))
            .optimize(input, context)
        val end = System.currentTimeMillis()
        LOG.info("optimize " + name + " cost " + (end - start) + " ms.")
        result
    }
  }

  /**
    * Gets program associated with the given name. If not found, return [[None]].
    */
  def get(name: String): Option[FlinkOptimizeProgram[OC]] = {
    val program = programMap.get(name)
    if (program.isDefined) {
      // using Option instead of Some to convert null to None
      Option(programMap(name))
    } else {
      None
    }
  }

  /**
    * Gets FlinkRuleSetProgram associated with the given name. If the program is not found or is
    * not a [[FlinkRuleSetProgram]], return [[None]].
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
    if (programNames.contains(name)) {
      false
    } else {
      programNames += name
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
    if (programNames.contains(name)) {
      false
    } else {
      programNames.insert(0, name)
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
    if (programNames.contains(name) || !programNames.contains(nameOfBefore)) {
      false
    } else if (programNames.isEmpty) {
      addLast(name, program)
    } else {
      val index = programNames.indexOf(nameOfBefore)
      programNames.insert(index, name)
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
    val index = programNames.indexOf(name)
    if (index >= 0) {
      programNames.remove(index)
    }
    programMap.remove(name)
  }

  /**
    * Returns program names with chained order.
    */
  def getProgramNames: util.List[String] = new util.ArrayList[String](programNames.asJava)

}
