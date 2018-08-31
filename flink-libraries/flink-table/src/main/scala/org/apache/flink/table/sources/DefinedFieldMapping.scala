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

import java.util.{Map => JMap}
import javax.annotation.Nullable

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sources.tsextractors.TimestampExtractor

/**
  * The [[DefinedFieldMapping]] interface provides a mapping for the fields of the table schema
  * ([[TableSource.getTableSchema]] to fields of the physical returned type
  * [[TableSource.getReturnType]] of a [[TableSource]].
  *
  * If a [[TableSource]] does not implement the [[DefinedFieldMapping]] interface, the fields of
  * its [[TableSchema]] are mapped to the fields of its return type [[TypeInformation]] by name.
  *
  * If the fields cannot or should not be implicitly mapped by name, an explicit mapping can be
  * provided by implementing this interface.
  * If a mapping is provided, all fields must be explicitly mapped.
  */
trait DefinedFieldMapping {

  /**
    * Returns the mapping for the fields of the [[TableSource]]'s [[TableSchema]] to the fields of
    * its return type [[TypeInformation]].
    *
    * The mapping is done based on field names, e.g., a mapping "name" -> "f1" maps the schema field
    * "name" to the field "f1" of the return type, for example in this case the second field of a
    * [[org.apache.flink.api.java.tuple.Tuple]].
    *
    * The returned mapping must map all fields (except proctime and rowtime fields) to the return
    * type. It can also provide a mapping for fields which are not in the [[TableSchema]] to make
    * fields in the physical [[TypeInformation]] accessible for a [[TimestampExtractor]].
    *
    * @return A mapping from [[TableSchema]] fields to [[TypeInformation]] fields or
    *         null if no mapping is necessary.
    */
  @Nullable
  def getFieldMapping: JMap[String, String]
}
