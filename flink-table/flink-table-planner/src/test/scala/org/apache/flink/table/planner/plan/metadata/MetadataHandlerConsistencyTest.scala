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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalCorrelate, BatchPhysicalGroupAggregateBase}
import org.apache.flink.table.planner.plan.utils.ReflectionsUtil

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Correlate}
import org.apache.calcite.rel.metadata.{MetadataDef, MetadataHandler, RelMetadataQuery}
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.reflect.Method
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Metadata estimation for logical RelNode and relative physical RelNode should be same.
  * Now almost every logical RelNode has same parent class with its relative physical RelNode except
  * for Aggregate and Correlate.
  * This test ensure two points.
  * 1. all subclasses of [[MetadataHandler]] have explicit metadata estimation
  * for [[Aggregate]] and [[BatchPhysicalGroupAggregateBase]] or have no metadata estimation for
  * [[Aggregate]] and [[BatchPhysicalGroupAggregateBase]] either.
  * 2. all subclasses of [[MetadataHandler]] have explicit metadata estimation
  * for [[Correlate]] and  [[BatchPhysicalGroupAggregateBase]] or have no metadata estimation for
  * [[Correlate]] and  [[BatchPhysicalGroupAggregateBase]] either.
  * Be cautious that if logical Aggregate and physical Aggregate or logical Correlate and physical
  * Correlate both are present in a MetadataHandler class, their metadata estimation should be same.
  * This test does not check this point because every MetadataHandler could have different
  * parameters with each other.
  */
@RunWith(classOf[Parameterized])
class MetadataHandlerConsistencyTest(
    logicalNodeClass: Class[_ <: RelNode],
    physicalNodeClass: Class[_ <: RelNode]) {

  // get all subclasses of [[MetadataHandler]]
  private val allMdHandlerClazz = fetchAllExtendedMetadataHandlers

  // initiate each subclasses of [[MetadataHandler]]
  private val mdHandlerInstances = allMdHandlerClazz map { mdhClass =>
    val constructor = mdhClass.getDeclaredConstructor()
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  // get [[MetadataDef]] of each subclasses of [[MetadataHandler]]
  private val mdDefMethods = allMdHandlerClazz.map(_.getMethod("getDef"))

  @Test
  def ensureLogicalNodeAndPhysicalNodeBothPresentOrBothAbsent(): Unit = {
    allMdHandlerClazz.zip(mdHandlerInstances).zip(mdDefMethods).foreach {
      case ((mdHandlerClass, mdHandlerInstance), mdDefMethod) =>
        val mdDef = mdDefMethod.invoke(mdHandlerInstance).asInstanceOf[MetadataDef[_]]
        val methodsInDef = mdDef.methods
        methodsInDef.foreach { methodInDef =>
          val logicalIsPresent = existExplicitEstimation(
            mdHandlerClass,
            logicalNodeClass,
            methodInDef)
          val physicalIsPresent = existExplicitEstimation(
            mdHandlerClass,
            physicalNodeClass,
            methodInDef)
          val mdHandlerClassName = mdHandlerClass.getCanonicalName
          // check logical node and physical node are both present or both absent
          if (!logicalIsPresent && physicalIsPresent) {
            fail(s"Require metadata estimation of ${logicalNodeClass.getCanonicalName} " +
              s"in $mdHandlerClassName!")
          } else if (logicalIsPresent && !physicalIsPresent) {
            fail(
              s"Require metadata estimation of ${physicalNodeClass.getCanonicalName} " +
                s"in $mdHandlerClassName")
          }
        }
    }
  }

  /**
    * Scan packages to find out all subclasses of [[MetadataHandler]] in flink.
    *
    * @return A list contains all subclasses of [[MetadataHandler]] in flink.
    */
  private def fetchAllExtendedMetadataHandlers: Seq[Class[_ <: MetadataHandler[_]]] = {
    ReflectionsUtil.scanSubClasses(
      "org.apache.flink.table.planner.plan.cost",
      classOf[MetadataHandler[_]]).toSeq
  }

  /**
    * Gets whether the given metadataHandler contains explicit metadata estimation for the given
    * RelNode class.
    *
    * @param mdHandlerClazz class of metadata handler
    * @param relNodeClazz   class of RelNode
    * @param methodInDef    metadata estimation method
    * @return True if the given metadataHandler contains explicit metadata estimation for the given
    * RelNode class, false else.
    */
  private def existExplicitEstimation(
      mdHandlerClazz: Class[_ <: MetadataHandler[_]],
      relNodeClazz: Class[_ <: RelNode],
      methodInDef: Method): Boolean = {
    val paramList: mutable.ListBuffer[Class[_]] = mutable.ListBuffer()
    paramList.append(relNodeClazz, classOf[RelMetadataQuery])
    paramList.appendAll(methodInDef.getParameterTypes)
    try {
      mdHandlerClazz.getMethod(methodInDef.getName, paramList: _*)
      true
    } catch {
      case _: NoSuchMethodException => false
    }
  }
}

object MetadataHandlerConsistencyTest {

  @Parameterized.Parameters(name = "logicalNodeClass={0}, physicalNodeClass={1}")
  def parameters(): util.Collection[Array[Any]] = {
    Seq[Array[Any]](
      Array(classOf[Aggregate], classOf[BatchPhysicalGroupAggregateBase]),
      Array(classOf[Correlate], classOf[BatchPhysicalCorrelate]))
  }
}
