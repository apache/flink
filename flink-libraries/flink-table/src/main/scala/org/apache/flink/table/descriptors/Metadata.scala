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

import org.apache.flink.table.descriptors.MetadataValidator.{METADATA_COMMENT, METADATA_CREATION_TIME, METADATA_LAST_ACCESS_TIME}

/**
  * Metadata descriptor for adding additional, useful information.
  */
class Metadata extends Descriptor {

  protected var comment: Option[String] = None
  protected var creationTime: Option[Long] = None
  protected var lastAccessTime: Option[Long] = None

  /**
    * Sets a comment.
    *
    * @param comment the description
    */
  def comment(comment: String): Metadata = {
    this.comment = Some(comment)
    this
  }

  /**
    * Sets a creation time.
    *
    * @param time UTC milliseconds timestamp
    */
  def creationTime(time: Long): Metadata = {
    this.creationTime = Some(time)
    this
  }

  /**
    * Sets a last access time.
    *
    * @param time UTC milliseconds timestamp
    */
  def lastAccessTime(time: Long): Metadata = {
    this.lastAccessTime = Some(time)
    this
  }

  /**
    * Internal method for properties conversion.
    */
  final override def addProperties(properties: DescriptorProperties): Unit = {
    comment.foreach(c => properties.putString(METADATA_COMMENT, c))
    creationTime.foreach(t => properties.putLong(METADATA_CREATION_TIME, t))
    lastAccessTime.foreach(t => properties.putLong(METADATA_LAST_ACCESS_TIME, t))
  }
}

/**
  * Metadata descriptor for adding additional, useful information.
  */
object Metadata {

  /**
    * Metadata descriptor for adding additional, useful information.
    */
  def apply(): Metadata = new Metadata()
}
