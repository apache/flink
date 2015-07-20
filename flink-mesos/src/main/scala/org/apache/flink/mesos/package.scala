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
package org.apache.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.{List => JList}

import com.google.common.base.Splitter
import com.google.protobuf.GeneratedMessage
import org.apache.mesos.Protos.{Attribute, Resource, Value}

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

package object mesos {
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }


  def getResource(res: JList[Resource], name: String): Double = {
    // A resource can have multiple values in the offer since it can either be from a specific role or wildcard.
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
   * Converts the attributes from the resource offer into a Map of name -> Attribute Value
   * The attribute values are the mesos attribute types and they are
   * @param offerAttributes
   * @return
   */
  def toAttributeMap(offerAttributes: JList[Attribute]): Map[String, GeneratedMessage] = {
    offerAttributes.map(attr => {
      val attrValue = attr.getType match {
        case Value.Type.SCALAR => attr.getScalar
        case Value.Type.RANGES => attr.getRanges
        case Value.Type.SET => attr.getSet
        case Value.Type.TEXT => attr.getText
      }
      (attr.getName, attrValue)
    }).toMap
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

  private val MEMORY_OVERHEAD_FRACTION = 0.10
  private val MEMORY_OVERHEAD_MINIMUM = 384

  /**
   * Return the amount of memory to allocate to each executor, taking into account container overheads.
   * @param conf Configuration to use to get the `memoryOverhead` value
   * @return memory requirement as (0.1 * <memoryOverhead>) or MEMORY_OVERHEAD_MINIMUM (whichever is larger)
   */
  def calculateTotalMemory(mem: Int, conf: Conf): Int = {
    val defaultOverhead = math.max(MEMORY_OVERHEAD_FRACTION * mem, MEMORY_OVERHEAD_MINIMUM).toInt
    conf.memoryOverhead.get.getOrElse(defaultOverhead) + mem
  }


  /**
   * Offsets the ports by the application id to avoid problems with multiple instances of Flink on Mesos and
   * thus multiple jobmanagers.
   * @param port Port to be offset.
   * @param appId AppId that the offset is calculated from.
   * @return
   */
  def offsetPort(port: Int, appId: Int): Int = {
    var p = port
    if (port > 65535) {
      p = 65535
    }

    if (p + (appId % 1000) > 65535) {
      p = p - 1000
    }

    p + (appId % 1000)
  }
}
