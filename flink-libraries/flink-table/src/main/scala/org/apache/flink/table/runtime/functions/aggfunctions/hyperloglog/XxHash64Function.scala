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

package org.apache.flink.table.runtime.functions.aggfunctions.hyperloglog

import java.sql.{Date => SqlDate, Time => SqlTime, Timestamp => SqlTimestamp}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.core.memory.{HeapMemorySegment, MemorySegment, MemorySegmentFactory}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.dataformat.{BinaryString, Decimal}
import org.apache.flink.table.util.hash.XXH64

object XxHash64Function {
  private def hashInt(i: Int, seed: Long): Long = XXH64.hashInt(i, seed)

  private def hashLong(l: Long, seed: Long): Long = XXH64.hashLong(l, seed)

  private def hashUnsafeBytes(base: MemorySegment, offset: Int,
                              length: Int, seed: Long): Long = {
    XXH64.hashUnsafeBytes(base, offset, length, seed)
  }

  /**
    * Computes hash of a given `value`. The caller needs to check the validity
    * of input `value`.
    */
  def hash(value: Any, seed: Long): Long = {
    value match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case d: SqlDate => hashLong(d.getTime, seed)
      case t: SqlTime => hashLong(t.getTime, seed)
      case t: SqlTimestamp => hashLong(t.getTime, seed)
      case d: JBigDecimal =>
        if (d.precision() <= Decimal.MAX_LONG_DIGITS) {
          hashLong(d.unscaledValue().longValueExact(), seed)
        } else {
          val bytes = d.unscaledValue().toByteArray
          val hms = HeapMemorySegment.FACTORY.wrap(bytes)
          hashUnsafeBytes(hms, 0, hms.size(), seed)
        }
      case s: BinaryString =>
        val segments = s.getSegments
        if (segments.length == 1) {
          hashUnsafeBytes(segments(0), s.getOffset, s.numBytes(), seed)
        } else {
          hashUnsafeBytes(MemorySegmentFactory.wrap(s.getBytes()), 0, s.numBytes(), seed)
        }
      case _ => throw new TableException(
        TableErrors.INST.sqlAggExecDataTypeNotSupported(
          "Approximate Count Distinct", value.toString))
    }
  }
}
