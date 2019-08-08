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

package org.apache.flink.table.descriptors

import java.util
import java.util.Collections

class TestTableDescriptor(connector: ConnectorDescriptor)
  extends TableDescriptor[TestTableDescriptor](connector)
  with SchematicDescriptor[TestTableDescriptor] {

  private var schemaDescriptor: Option[Schema] = None

  override def withSchema(schema: Schema): TestTableDescriptor = {
    this.schemaDescriptor = Some(schema)
    this
  }

  override protected def additionalProperties(): util.Map[String, String] = {
    schemaDescriptor match {
      case Some(d) => d.toProperties
      case None => Collections.emptyMap()
    }
  }
}
