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

package org.apache.flink.table.expressions

import org.apache.flink.table.`type`.{InternalType, InternalTypes, TimestampType}
import org.apache.flink.table.api.TableException

trait WindowProperty {
  def resultType: InternalType
}

abstract class AbstractWindowProperty(reference: WindowReference) extends WindowProperty {
  override def toString = s"WindowProperty($reference)"
}

/**
  * Indicate timeField type.
  */
case class WindowReference(name: String, tpe: Option[InternalType] = None) {
  override def toString: String = s"'$name"
}

case class WindowStart(reference: WindowReference) extends AbstractWindowProperty(reference) {

  override def resultType: TimestampType = InternalTypes.TIMESTAMP

  override def toString: String = s"start($reference)"
}

case class WindowEnd(reference: WindowReference) extends AbstractWindowProperty(reference) {

  override def resultType: TimestampType = InternalTypes.TIMESTAMP

  override def toString: String = s"end($reference)"
}

case class RowtimeAttribute(reference: WindowReference) extends AbstractWindowProperty(reference) {

  override def resultType: InternalType = {
    reference match {
      case WindowReference(_, Some(tpe)) if tpe == InternalTypes.ROWTIME_INDICATOR =>
        // rowtime window
        InternalTypes.ROWTIME_INDICATOR
      case WindowReference(_, Some(tpe))
        if tpe == InternalTypes.LONG || tpe == InternalTypes.TIMESTAMP =>
        // batch time window
        InternalTypes.TIMESTAMP
      case _ =>
        throw new TableException("WindowReference of RowtimeAttribute has invalid type. " +
            "Please report this bug.")
    }
  }

  override def toString: String = s"rowtime($reference)"
}

case class ProctimeAttribute(reference: WindowReference)
  extends AbstractWindowProperty(reference) {

  override def resultType: InternalType = InternalTypes.PROCTIME_INDICATOR

  override def toString: String = s"proctime($reference)"
}
