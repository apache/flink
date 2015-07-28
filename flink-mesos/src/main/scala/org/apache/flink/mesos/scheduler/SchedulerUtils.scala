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

package org.apache.flink.mesos.scheduler

import java.util.{List => JList}

import com.google.common.base.Splitter
import com.google.protobuf.{ByteString, GeneratedMessage}
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos._
import org.apache.mesos.Protos.CommandInfo.URI
import org.apache.mesos.Protos.Value.Ranges
import org.apache.mesos.Protos.Value.Type._
import org.apache.mesos.Protos._

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

trait SchedulerUtils {

  /**
   * Converts the attributes from the resource offer into a Map of name -> Attribute Value
   * The attribute values are the mesos attribute types and they are
   * @param offerAttributes List of attributes sent with an offer
   * @return
   */
  def toAttributeMap(offerAttributes: JList[Attribute]): Map[String, GeneratedMessage] = {
    offerAttributes.map(attr => {
      val attrValue = attr.getType match {
        case SCALAR => attr.getScalar
        case Value.Type.RANGES => attr.getRanges
        case Value.Type.SET => attr.getSet
        case Value.Type.TEXT => attr.getText
      }
      (attr.getName, attrValue)
    }).toMap
  }

  def meetsConstraints(requiredMem: Int, requiredCPUs: Int,
                       constraints: Map[String, Set[String]])(o: Offer): Boolean = {
    val mem = getResource(o.getResourcesList, "mem")
    val cpu = getResource(o.getResourcesList, "cpu")
    val offerAttributes = toAttributeMap(o.getAttributesList)

    // check if all constraints are satisfield
    //  1. Attribute constraints
    //  2. Memory requirements
    //  3. CPU requirements - need at least 1 for executor, 1 for task
    val meetsConstraints = matchesAttributeRequirements(constraints, offerAttributes)
    val meetsMemoryRequirements = mem >= requiredMem
    val meetsCPURequirements = cpu >= requiredCPUs

    meetsConstraints && meetsMemoryRequirements && meetsCPURequirements
  }

  def createJavaExecCommand(jvmArgs: String = "", classPath: String = ".",
                            classToExecute: String, args: String = ""): String = {
    s"java $jvmArgs -cp $classPath $classToExecute $args"
  }

  def createExecutorInfo(id: String, role: String, mem: Double, cpus: Double, disk: Double,
                         ports: Set[Int], artifactURIs: Set[String], command: String,
                         nativeLibPath: String): ExecutorInfo = {
    val portRanges = Ranges.newBuilder().addAllRange(
      ports.map(port => Value.Range.newBuilder().setBegin(port).setEnd(port).build()))
    val uris = artifactURIs.map(uri => URI.newBuilder().setValue(uri).build())

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID
        .newBuilder()
        .setValue(s"executor_$id"))
      .setName("Apache Flink Mesos Executor")
      .setSource(s"task_$id")
      .addResources(Resource.newBuilder()
        .setName("ports").setType(RANGES)
        .setRole(role)
        .setRanges(portRanges))
      .addResources(Resource.newBuilder()
        .setName("mem").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(mem)))
      .addResources(Resource.newBuilder()
        .setName("cpus").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(cpus)))
      .addResources(Resource.newBuilder()
        .setName("disk").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(disk)))
      .setCommand(CommandInfo.newBuilder()
        .setValue(s"env; $command")
        .addAllUris(uris)
        .setEnvironment(Environment.newBuilder()
          .addVariables(Environment.Variable.newBuilder()
            .setName("MESOS_NATIVE_JAVA_LIBRARY").setValue(nativeLibPath)))
        .setValue(command))
      .build()
  }

  def createTaskInfo(taskName: String, taskId: TaskID, slaveID: SlaveID, role: String, mem: Double,
                     cpus: Double, ports: Set[Int], executorInfo: ExecutorInfo,
                     conf: Configuration): TaskInfo = {

    val portRanges = Ranges.newBuilder().addAllRange(
      ports.map(port => Value.Range.newBuilder().setBegin(port).setEnd(port).build())).build()

    TaskInfo.newBuilder()
      .setData(ByteString.copyFrom(Utils.serialize(conf)))
      .setExecutor(executorInfo)
      .setName(taskId.getValue)
      .setSlaveId(slaveID)
      .addResources(Resource.newBuilder()
        .setName("ports").setType(RANGES)
        .setRole(role)
        .setRanges(portRanges))
      .addResources(Resource.newBuilder()
        .setName("mem").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(mem)))
      .addResources(Resource.newBuilder()
        .setName("cpus").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(cpus)))
      .build()
  }


  def getResource(res: JList[Resource], name: String): Double = {
    // A resource can have multiple values in the offer since it can
    // either be from a specific role or wildcard.
    res.filter(_.getName == name).map(_.getScalar.getValue).sum
  }

  /**
   * Match the requirements (if any) to the offer attributes.
   * if attribute requirements are not specified - return true
   * else if attribute is defined and no values are given, simple attribute presence is performed
   * else if attribute name and value is specified, subset match is performed on slave attributes
   */
  def matchesAttributeRequirements(
                                    slaveOfferConstraints: Map[String, Set[String]],
                                    offerAttributes: Map[String, GeneratedMessage]): Boolean = {
    slaveOfferConstraints.forall {
      // offer has the required attribute and subsumes the required values for that attribute
      case (name, requiredValues) =>
        offerAttributes.get(name) match {
          case None => false
          case Some(_) if requiredValues.isEmpty => true // empty value matches presence
          case Some(scalarValue: Value.Scalar) =>
            // check if provided values is less than equal to the offered values
            requiredValues.map(_.toDouble).exists(_ <= scalarValue.getValue)
          case Some(rangeValue: Value.Range) =>
            val offerRange = rangeValue.getBegin to rangeValue.getEnd
            // Check if there is some required value that is between the ranges specified
            // Note: We only support the ability to specify discrete values, in the future
            // we may expand it to subsume ranges specified with a XX..YY value or something
            // similar to that.
            requiredValues.map(_.toLong).exists(offerRange.contains(_))
          case Some(offeredValue: Value.Set) =>
            // check if the specified required values is a subset of offered set
            requiredValues.subsetOf(offeredValue.getItemList.toSet)
          case Some(textValue: Value.Text) =>
            // check if the specified value is equal, if multiple values are specified
            // we succeed if any of them match.
            requiredValues.contains(textValue.getValue)
        }
    }
  }

  /**
   * Parses the attributes constraints provided to spark and build a matching data struct:
   * Map[<attribute-name>, Set[values-to-match]]
   * The constraints are specified as ';' separated key-value pairs where keys and values
   * are separated by ':'. The ':' implies equality (for singular values) and "is one of" for
   * multiple values (comma separated). For example:
   * {{{
   *  parseConstraintString("tachyon:true;zone:us-east-1a,us-east-1b")
   *  // would result in
   * <code>
   * Map(
   * "tachyon" -> Set("true"),
   * "zone":   -> Set("us-east-1a", "us-east-1b")
   * )
   * }}}
   *
   * Mesos documentation: http://mesos.apache.org/documentation/attributes-resources/
   * https://github.com/apache/mesos/blob/master/src/common/values.cpp
   * https://github.com/apache/mesos/blob/master/src/common/attributes.cpp
   *
   * @param constraintsVal constaints string consisting of ';' separated key-value pairs (separated
   * by ':')
   * @return  Map of constraints to match resources offers.
   */
  def parseConstraintString(constraintsVal: String): Map[String, Set[String]] = {
    /*
      Based on mesos docs:
      attributes : attribute ( ";" attribute )*
      attribute : labelString ":" ( labelString | "," )+
      labelString : [a-zA-Z0-9_/.-]
    */
    val splitter = Splitter.on(';').trimResults().withKeyValueSeparator(':')
    // kv splitter
    if (constraintsVal.isEmpty) {
      Map()
    } else {
      try {
        Map() ++ mapAsScalaMap(splitter.split(constraintsVal)).map {
          case (k, v) =>
            if (v == null || v.isEmpty) {
              (k, Set[String]())
            } else {
              (k, v.split(',').toSet)
            }
        }
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Bad constraint string: $constraintsVal", e)
      }
    }
  }

}
