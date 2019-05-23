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

import org.apache.flink.table.descriptors.FileSystemValidator.{CONNECTOR_PATH, CONNECTOR_TYPE_VALUE}

/**
  * Connector descriptor for a file system.
  */
class FileSystem extends ConnectorDescriptor(CONNECTOR_TYPE_VALUE, 1, true) {

  private var path: Option[String] = None

  /**
    * Sets the path to a file or directory in a file system.
    *
    * @param path the path a file or directory
    */
  def path(path: String): FileSystem = {
    this.path = Some(path)
    this
  }

  override protected def toConnectorProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()

    path.foreach(properties.putString(CONNECTOR_PATH, _))

    properties.asMap()
  }
}

/**
  * Connector descriptor for a file system.
  */
object FileSystem {

  /**
    * Connector descriptor for a file system.
    *
    * @deprecated Use `new FileSystem()`.
    */
  @deprecated
  def apply(): FileSystem = new FileSystem()
  
}
