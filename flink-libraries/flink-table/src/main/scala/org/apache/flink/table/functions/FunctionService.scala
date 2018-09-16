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

package org.apache.flink.table.functions

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors._
import org.apache.flink.table.util.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Service for creating configured instances of [[UserDefinedFunction]] using a
  * [[FunctionDescriptor]].
  */
object FunctionService extends Logging {

  /**
    * Creates a user-defined function with the given properties and the current thread's
    * context class loader.
    *
    * @param descriptor the descriptor that describes a function
    * @return the generated user-defined function
    */
  def createFunction(descriptor: FunctionDescriptor): UserDefinedFunction = {
    createFunction(descriptor, Thread.currentThread().getContextClassLoader)
  }

  /**
    * Creates a user-defined function with the given properties.
    *
    * @param descriptor the descriptor that describes a function
    * @param classLoader the class loader to load the function and its parameter's classes
    * @return the generated user-defined function
    */
  def createFunction(
      descriptor: FunctionDescriptor,
      classLoader: ClassLoader)
    : UserDefinedFunction = {

    val descriptorProperties = new DescriptorProperties(true)
    descriptor.addProperties(descriptorProperties)

    // validate
    new FunctionDescriptorValidator().validate(descriptorProperties)

    // instantiate
    val (instanceClass, instance) = generateInstance[AnyRef](
      HierarchyDescriptorValidator.EMPTY_PREFIX,
      descriptorProperties,
      classLoader)

    if (!classOf[UserDefinedFunction].isAssignableFrom(instanceClass)) {
      throw new ValidationException(
        s"Instantiated class '${instanceClass.getName}' is not a user-defined function.")
    }
    instance.asInstanceOf[UserDefinedFunction]
  }

  /**
   * Recursively generate an instance of a class according the given properties.
   *
   * @param keyPrefix the prefix to fetch properties
   * @param descriptorProperties the descriptor properties that contains the class type information
   * @param classLoader the class loader to load the class
   * @tparam T type fo the generated instance
   * @return an instance of the class
   */
  private def generateInstance[T](
      keyPrefix: String,
      descriptorProperties: DescriptorProperties,
      classLoader: ClassLoader)
    : (Class[T], T) = {

    val instanceClassName = descriptorProperties.getString(
      s"$keyPrefix${ClassInstanceValidator.CLASS}")

    val instanceClass = try {
      Class
        .forName(
          descriptorProperties.getString(s"$keyPrefix${ClassInstanceValidator.CLASS}"),
          true,
          classLoader)
        .asInstanceOf[Class[T]]
    } catch {
      case e: Exception =>
        // only log the cause to have clean error messages
        val msg = s"Could not find class '$instanceClassName' for creating an instance."
        LOG.error(msg, e)
        throw new ValidationException(msg)
    }

    val constructorPrefix = s"$keyPrefix${ClassInstanceValidator.CONSTRUCTOR}"

    val constructorProps = descriptorProperties
      .getVariableIndexedProperties(constructorPrefix, List())

    var i = 0
    val parameterList: ArrayBuffer[(Class[_], Any)] = new ArrayBuffer
    while (i < constructorProps.size()) {
      // nested class instance
      if (constructorProps(i).containsKey(ClassInstanceValidator.CLASS)) {
        parameterList += generateInstance(
          s"$constructorPrefix.$i.",
          descriptorProperties,
          classLoader)
      }
      // literal value
      else {
        val literalValue = LiteralValueValidator
          .getValue(s"$constructorPrefix.$i.", descriptorProperties)
        parameterList += ((literalValue.getClass, literalValue))
      }
      i += 1
    }
    val constructor = try {
      instanceClass.getConstructor(parameterList.map(_._1): _*)
    } catch {
      case e: Exception =>
        // only log the cause to have clean error messages
        val msg = s"Cannot find a public constructor with parameter types " +
          s"'${parameterList.map(_._1.getName).mkString(", ")}' for '$instanceClassName'."
        LOG.error(msg, e)
        throw new ValidationException(msg)
    }

    val instance = try {
      constructor.newInstance(parameterList.map(_._2.asInstanceOf[AnyRef]): _*)
    } catch {
      case e: Exception =>
        // only log the cause to have clean error messages
        val msg = s"Error while creating instance of class '$instanceClassName' " +
          s"with parameter types '${parameterList.map(_._1.getName).mkString(", ")}'."
        LOG.error(msg, e)
        throw new ValidationException(msg)
    }

    (instanceClass, instance)
  }
}
