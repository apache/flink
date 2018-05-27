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

package org.apache.flink.table.descriptors.service

import org.apache.flink.table.api.TableException
import org.apache.flink.table.descriptors.{ClassTypeValidator, DescriptorProperties, FunctionDescriptor, FunctionValidator, PrimitiveTypeValidator}
import org.apache.flink.table.functions.UserDefinedFunction

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Utils that serve [[FunctionDescriptor]].
  */
object FunctionService {
   /**
     * Generates a user-defined function with the given properties.
     *
     * @param properties the descriptor properties that belongs to a [[FunctionDescriptor]]
     * @param classLoader the class loader to load the function and its parameter's classes
     * @return the generated user-defined function
     */
   def generateUserDefinedFunction(
      properties: DescriptorProperties,
      classLoader: ClassLoader): UserDefinedFunction = {
      new FunctionValidator().validate(properties)
      generateInstance[UserDefinedFunction]("", properties, classLoader)
   }

   /**
     * Recursively generate an instance of a class according the given properties.
     *
     * @param keyPrefix the prefix to fetch properties
     * @param descriptorProps the descriptor properties that contains the class type information
     * @param classLoader the class loader to load the class
     * @tparam T type fo the generated instance
     * @return an instance of the class
     */
   def generateInstance[T](
      keyPrefix: String,
      descriptorProps: DescriptorProperties,
      classLoader: ClassLoader): T = {
      val constructorPrefix = s"$keyPrefix${ClassTypeValidator.CONSTRUCTOR}"
      val constructorProps =
         descriptorProps.getVariableIndexedProperties(constructorPrefix, List())
      var i = 0
      val typeValueList: ArrayBuffer[(Class[_], Any)] = new ArrayBuffer
      while (i < constructorProps.size()) {
         if (constructorProps(i).containsKey(PrimitiveTypeValidator.PRIMITIVE_TYPE)) {
            val primitiveVal = PrimitiveTypeValidator
              .derivePrimitiveValue(s"$constructorPrefix.$i.", descriptorProps)
            typeValueList += ((primitiveVal.getClass, primitiveVal))
         } else if (constructorProps(i).containsKey(ClassTypeValidator.CLASS)) {
            val typeValuePair = (
              Class.forName(
                 descriptorProps.getString(constructorProps(i).get(ClassTypeValidator.CLASS))),
              generateInstance(s"$constructorPrefix.$i.", descriptorProps, classLoader))
            typeValueList += typeValuePair
         }
         i += 1
      }
      val clazz = classLoader
        .loadClass(descriptorProps.getString(s"$keyPrefix${ClassTypeValidator.CLASS}"))
      val constructor = clazz.getConstructor(typeValueList.map(_._1): _*)
      if (null == constructor) {
         throw TableException(s"Cannot find a constructor with parameter types " +
           s"${typeValueList.map(_._1)} for ${clazz.getName}")
      }
      constructor.newInstance(typeValueList.map(_._2.asInstanceOf[AnyRef]): _*).asInstanceOf[T]
   }
}
