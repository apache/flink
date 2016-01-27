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

package org.apache.flink.ml.common

import scala.collection.mutable

/**
 * Map used to store configuration parameters for algorithms. The parameter
 * values are stored in a [[Map]] being identified by a [[Parameter]] object. ParameterMaps can
 * be fused. This operation is left associative, meaning that latter ParameterMaps can override
 * parameter values defined in a preceding ParameterMap.
 *
 * @param map Map containing parameter settings
 */
class ParameterMap(val map: mutable.Map[Parameter[_], Any]) extends Serializable {

  def this() = {
    this(new mutable.HashMap[Parameter[_], Any]())
  }

  /**
   * Adds a new parameter value to the ParameterMap.
   *
   * @param parameter Key
   * @param value Value associated with the given key
   * @tparam T Type of value
   */
  def add[T](parameter: Parameter[T], value: T): ParameterMap = {
    map += (parameter -> value)
    this
  }

  /**
   * Retrieves a parameter value associated to a given key. The value is returned as an Option.
   * If there is no value associated to the given key, then the default value of the [[Parameter]]
   * is returned.
   *
   * @param parameter Key
   * @tparam T Type of the value to retrieve
   * @return Some(value) if an value is associated to the given key, otherwise the default value
   *         defined by parameter
   */
  def get[T](parameter: Parameter[T]): Option[T] = {
    if(map.isDefinedAt(parameter)) {
      map.get(parameter).asInstanceOf[Option[T]]
    } else {
      parameter.defaultValue
    }
  }

  /**
   *  Retrieves a parameter value associated to a given key. If there is no value contained in the
   *  map, then the default value of the [[Parameter]] is checked. If the default value is defined,
   *  then it is returned. If the default is undefined, then a [[NoSuchElementException]] is thrown.
   *
   * @param parameter Key
   * @tparam T Type of value
   * @return Value associated with the given key or its default value
   */
  def apply[T](parameter: Parameter[T]): T = {
    if(map.isDefinedAt(parameter)) {
      map(parameter).asInstanceOf[T]
    } else {
      parameter.defaultValue match {
        case Some(value) => value
        case None => throw new NoSuchElementException(s"Could not retrieve " +
          s"parameter value $parameter.")
      }
    }
  }

  /**
   * Adds the parameter values contained in parameters to itself.
   *
   * @param parameters [[ParameterMap]] containing the parameter values to be added
   * @return this after inserting the parameter values from parameters
   */
  def ++(parameters: ParameterMap): ParameterMap = {
    val result = new ParameterMap(map)
    result.map ++= parameters.map

    result
  }
}

object ParameterMap {
  val Empty = new ParameterMap

  def apply(): ParameterMap = {
    new ParameterMap
  }
}

/**
 * Base trait for parameter keys
 *
 * @tparam T Type of parameter value associated to this parameter key
 */
trait Parameter[T] {

  /**
   * Default value of parameter. If no such value exists, then returns [[None]]
   */
  val defaultValue: Option[T]
}
