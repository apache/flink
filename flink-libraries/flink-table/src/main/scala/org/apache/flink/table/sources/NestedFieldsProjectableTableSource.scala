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

package org.apache.flink.table.sources

/**
  * Adds support for projection push-down to a [[TableSource]] with nested fields.
  * A [[TableSource]] extending this interface is able
  * to project the nested fields of the returned table.
  *
  * @tparam T The return type of the [[NestedFieldsProjectableTableSource]].
  */
trait NestedFieldsProjectableTableSource[T] {

  /**
    * Creates a copy of the [[TableSource]] that projects its output on the specified nested fields.
    *
    * @param fields The indexes of the fields to return.
    * @param nestedFields The accessed nested fields of the fields to return.
    *
    * e.g.
    * tableSchema = {
    *       id,
    *       student<\school<\city, tuition>, age, name>,
    *       teacher<\age, name>
    *       }
    *
    * select (id, student.school.city, student.age, teacher)
    *
    * fields = field = [0, 1, 2]
    * nestedFields  \[\["*"], ["school.city", "age"], ["*"\]\]
    *
    * @return A copy of the [[TableSource]] that projects its output.
    */
  def projectNestedFields(
      fields: Array[Int],
      nestedFields: Array[Array[String]]): TableSource[T]

}
