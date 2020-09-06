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

package org.apache.flink.table.utils

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.collection.JavaConverters._
import scala.collection.mutable

class InputTypeBuilder(typeFactory: JavaTypeFactory) {

  private val names = mutable.ListBuffer[String]()
  private val types = mutable.ListBuffer[RelDataType]()

  def field(name: String, `type`: SqlTypeName): InputTypeBuilder = {
    names += name
    types += typeFactory.createSqlType(`type`)
    this
  }

  def nestedField(name: String, `type`: RelDataType): InputTypeBuilder = {
    names += name
    types += `type`
    this
  }

  def build: RelDataType = {
    typeFactory.createStructType(types.asJava, names.asJava)
  }
}

object InputTypeBuilder {

  def inputOf(typeFactory: JavaTypeFactory) = new InputTypeBuilder(typeFactory)
}
