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

package org.apache.flink.table.planner.codegen.sort

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.{DecimalData, TimestampData}
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.codegen.CodeGenUtils.{ROW_DATA, SEGMENT, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec
import org.apache.flink.table.runtime.generated.{GeneratedNormalizedKeyComputer, GeneratedRecordComparator, NormalizedKeyComputer, RecordComparator}
import org.apache.flink.table.runtime.operators.sort.SortUtil
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{DecimalType, LogicalType, RowType, TimestampType}

import scala.collection.mutable

/**
  * A code generator for generating [[NormalizedKeyComputer]] and [[RecordComparator]].
  *
  * @param conf         config of the planner.
  * @param input        input type.
  * @param sortSpec     sort specification.
  */
class SortCodeGenerator(
    conf: TableConfig,
    val input: RowType,
    val sortSpec: SortSpec) {

  private val MAX_NORMALIZED_KEY_LEN = 16

  private val SORT_UTIL = classOf[SortUtil].getCanonicalName

  /** Chunks for long, int, short, byte */
  private val POSSIBLE_CHUNK_SIZES = Array(8, 4, 2, 1)

  /** For get${operator} set${operator} of [[org.apache.flink.core.memory.MemorySegment]] */
  private val BYTE_OPERATOR_MAPPING = Map(8 -> "Long", 4 -> "Int", 2 -> "Short", 1 -> "")

  /** For primitive define */
  private val BYTE_DEFINE_MAPPING = Map(8 -> "long", 4 -> "int", 2 -> "short", 1 -> "byte")

  /** For Class of primitive type */
  private val BYTE_CLASS_MAPPING = Map(8 -> "Long", 4 -> "Integer", 2 -> "Short", 1 -> "Byte")

  /** Normalized meta */
  val (nullAwareNormalizedKeyLen, normalizedKeyNum, invertNormalizedKey, normalizedKeyLengths) = {
    var keyLen = 0
    var keyNum = 0
    var inverted = false
    val keyLengths = new mutable.ArrayBuffer[Int]
    var break = false
    var i = 0
    while (i < sortSpec.getFieldSize && !break) {
      val fieldSpec = sortSpec.getFieldSpec(i)
      val t = input.getTypeAt(fieldSpec.getFieldIndex)
      if (supportNormalizedKey(t)) {
        val invert = !fieldSpec.getIsAscendingOrder

        if (i == 0) {
          // the first comparator decides whether we need to invert the key direction
          inverted = invert
        }

        if (invert != inverted) {
          // if a successor does not agree on the inversion direction,
          // it cannot be part of the normalized key
          break = true
        } else {
          keyNum += 1
          // Need add null aware 1 byte
          val len = safeAddLength(getNormalizeKeyLen(t), 1)
          if (len < 0) {
            throw new RuntimeException(
              s"$t specifies an invalid length for the normalized key: " + len)
          }
          keyLengths += len
          keyLen = safeAddLength(keyLen, len)
          if (keyLen == Integer.MAX_VALUE) {
            break = true
          }
        }
      } else {
        break = true
      }
      i += 1
    }

    (keyLen, keyNum, inverted, keyLengths)
  }

  def getKeyFullyDeterminesAndBytes: (Boolean, Int) = {
    if (nullAwareNormalizedKeyLen > 18) {
      // The maximum setting is 18 because want to put two null aware long as much as possible.
      // Anyway, we can't fit it, so align the most efficient 8 bytes.
      (false, Math.min(MAX_NORMALIZED_KEY_LEN, 8 * normalizedKeyNum))
    } else {
      (normalizedKeyNum == sortSpec.getFieldSize, nullAwareNormalizedKeyLen)
    }
  }

  /**
    * Generates a [[NormalizedKeyComputer]] that can be passed to a Java compiler.
    *
    * @param name Class name of the function.
    *             Does not need to be unique but has to be a valid Java class identifier.
    * @return A GeneratedNormalizedKeyComputer
    */
  def generateNormalizedKeyComputer(name: String): GeneratedNormalizedKeyComputer = {

    val className = newName(name)

    val (keyFullyDetermines, numKeyBytes) = getKeyFullyDeterminesAndBytes

    val putKeys = generatePutNormalizedKeys(numKeyBytes)

    val chunks = calculateChunks(numKeyBytes)

    val reverseKeys = generateReverseNormalizedKeys(chunks)

    val compareKeys = generateCompareNormalizedKeys(chunks)

    val swapKeys = generateSwapNormalizedKeys(chunks)

    val baseClass = classOf[NormalizedKeyComputer]

    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        public $className(Object[] references) {
          // useless
        }

        @Override
        public void putKey($ROW_DATA record, $SEGMENT target, int offset) {
          ${putKeys.mkString}
          ${reverseKeys.mkString}
        }

        @Override
        public int compareKey($SEGMENT segI, int offsetI, $SEGMENT segJ, int offsetJ) {
          ${compareKeys.mkString}
        }

        @Override
        public void swapKey($SEGMENT segI, int offsetI, $SEGMENT segJ, int offsetJ) {
          ${swapKeys.mkString}
        }

        @Override
        public int getNumKeyBytes() {
          return $numKeyBytes;
        }

        @Override
        public boolean isKeyFullyDetermines() {
          return $keyFullyDetermines;
        }

        @Override
        public boolean invertKey() {
          return $invertNormalizedKey;
        }

      }
    """.stripMargin

    new GeneratedNormalizedKeyComputer(className, code, conf.getConfiguration)
  }

  def generatePutNormalizedKeys(numKeyBytes: Int): mutable.ArrayBuffer[String] = {
    /* Example generated code, for int:
    if (record.isNullAt(0)) {
      org.apache.flink.table.data.binary.BinaryRowDataUtil.minNormalizedKey(target, offset+0, 5);
    } else {
      target.put(offset+0, (byte) 1);
      org.apache.flink.table.data.binary.BinaryRowDataUtil.putIntNormalizedKey(
        record.getInt(0), target, offset+1, 4);
    }
     */
    val putKeys = new mutable.ArrayBuffer[String]
    var bytesLeft = numKeyBytes
    var currentOffset = 0
    var keyIndex = 0
    while (bytesLeft > 0 && keyIndex < normalizedKeyNum) {
      var len = normalizedKeyLengths(keyIndex)
      val fieldSpec = sortSpec.getFieldSpec(keyIndex)
      val index = fieldSpec.getFieldIndex
      val nullIsMaxValue = fieldSpec.getIsAscendingOrder == fieldSpec.getNullIsLast
      len = if (bytesLeft >= len) len else bytesLeft
      val t = input.getTypeAt(fieldSpec.getFieldIndex)
      val prefix = prefixGetFromBinaryRow(t)
      val putCode = t match {
        case _ if getNormalizeKeyLen(t) != Int.MaxValue =>
          val get = getter(t, index)
          s"""
             |target.put(offset+$currentOffset, (byte) 1);
             |$SORT_UTIL.put${prefixPutNormalizedKey(t)}NormalizedKey(
             |  record.$get, target, offset+${currentOffset + 1}, ${len - 1});
             |
         """.stripMargin
        case _ =>
          // It is StringData/byte[].., we can omit the null aware byte(zero is the smallest),
          // because there is no other field behind, and is not keyFullyDetermines.
          s"""
             |$SORT_UTIL.put${prefixPutNormalizedKey(t)}NormalizedKey(
             |  record.get$prefix($index), target, offset+$currentOffset, $len);
             |""".stripMargin
      }
      val nullCode = if (nullIsMaxValue) {
        s"$SORT_UTIL.maxNormalizedKey(target, offset+$currentOffset, $len);"
      } else {
        s"$SORT_UTIL.minNormalizedKey(target, offset+$currentOffset, $len);"
      }

      val code =
        s"""
           |if (record.isNullAt($index)) {
           | $nullCode
           |} else {
           | $putCode
           |}
           |""".stripMargin

      putKeys += code
      bytesLeft -= len
      currentOffset += len
      keyIndex += 1
    }
    putKeys
  }

  /**
    * In order to better performance and not use MemorySegment's compare() and swap(),
    * we CodeGen more efficient chunk method.
    */
  def calculateChunks(numKeyBytes: Int): Array[Int] = {
    /* Example chunks, for int:
      calculateChunks(5) = Array(4, 1)
     */
    val chunks = new mutable.ArrayBuffer[Int]
    var i = 0
    var remainBytes = numKeyBytes
    while (remainBytes > 0) {
      val bytes = POSSIBLE_CHUNK_SIZES(i)
      if (bytes <= remainBytes) {
        chunks += bytes
        remainBytes -= bytes
      } else {
        i += 1
      }
    }
    chunks.toArray
  }

  /**
    * Because we put normalizedKeys in big endian way, if we are the little endian,
    * we need to reverse these data with chunks for comparation.
    */
  def generateReverseNormalizedKeys(chunks: Array[Int]): mutable.ArrayBuffer[String] = {
    /* Example generated code, for int:
    target.putInt(offset+0, Integer.reverseBytes(target.getInt(offset+0)));
    //byte don't need reverse.
     */
    val reverseKeys = new mutable.ArrayBuffer[String]
    // If it is big endian, it would be better, no reverse.
    if (BinaryRowData.LITTLE_ENDIAN) {
      var reverseOffset = 0
      for (chunk <- chunks) {
        val operator = BYTE_OPERATOR_MAPPING(chunk)
        val className = BYTE_CLASS_MAPPING(chunk)
        if (chunk != 1) {
          val reverseKey =
            s"""
               |target.put$operator(offset+$reverseOffset,
               |  $className.reverseBytes(target.get$operator(offset+$reverseOffset)));
            """.stripMargin
          reverseKeys += reverseKey
        }
        reverseOffset += chunk
      }
    }
    reverseKeys
  }

  /**
    * Compare bytes with chunks and nsigned.
    */
  def generateCompareNormalizedKeys(chunks: Array[Int]): mutable.ArrayBuffer[String] = {
    /* Example generated code, for int:
    int l_0_1 = segI.getInt(offsetI+0);
    int l_0_2 = segJ.getInt(offsetJ+0);
    if (l_0_1 != l_0_2) {
      return ((l_0_1 < l_0_2) ^ (l_0_1 < 0) ^
        (l_0_2 < 0) ? -1 : 1);
    }

    byte l_1_1 = segI.get(offsetI+4);
    byte l_1_2 = segJ.get(offsetJ+4);
    if (l_1_1 != l_1_2) {
      return ((l_1_1 < l_1_2) ^ (l_1_1 < 0) ^
        (l_1_2 < 0) ? -1 : 1);
    }
    return 0;
     */
    val compareKeys = new mutable.ArrayBuffer[String]
    var compareOffset = 0
    for (i <- chunks.indices) {
      val chunk = chunks(i)
      val operator = BYTE_OPERATOR_MAPPING(chunk)
      val define = BYTE_DEFINE_MAPPING(chunk)
      val compareKey =
        s"""
           |$define l_${i}_1 = segI.get$operator(offsetI+$compareOffset);
           |$define l_${i}_2 = segJ.get$operator(offsetJ+$compareOffset);
           |if (l_${i}_1 != l_${i}_2) {
           |  return ((l_${i}_1 < l_${i}_2) ^ (l_${i}_1 < 0) ^
           |    (l_${i}_2 < 0) ? -1 : 1);
           |}
            """.stripMargin
      compareKeys += compareKey
      compareOffset += chunk
    }
    compareKeys += "return 0;"
    compareKeys
  }

  /**
    * Swap bytes with chunks.
    */
  def generateSwapNormalizedKeys(chunks: Array[Int]): mutable.ArrayBuffer[String] = {
    /* Example generated code, for int:
    int temp0 = segI.getInt(offsetI+0);
    segI.putInt(offsetI+0, segJ.getInt(offsetJ+0));
    segJ.putInt(offsetJ+0, temp0);

    byte temp1 = segI.get(offsetI+4);
    segI.put(offsetI+4, segJ.get(offsetJ+4));
    segJ.put(offsetJ+4, temp1);
     */
    val swapKeys = new mutable.ArrayBuffer[String]
    var swapOffset = 0
    for (i <- chunks.indices) {
      val chunk = chunks(i)
      val operator = BYTE_OPERATOR_MAPPING(chunk)
      val define = BYTE_DEFINE_MAPPING(chunk)
      val swapKey =
        s"""
           |$define temp$i = segI.get$operator(offsetI+$swapOffset);
           |segI.put$operator(offsetI+$swapOffset, segJ.get$operator(offsetJ+$swapOffset));
           |segJ.put$operator(offsetJ+$swapOffset, temp$i);
            """.stripMargin
      swapKeys += swapKey
      swapOffset += chunk
    }
    swapKeys
  }

  /**
    * Generates a [[RecordComparator]] that can be passed to a Java compiler.
    *
    * @param name Class name of the function.
    *             Does not need to be unique but has to be a valid Java class identifier.
    * @return A GeneratedRecordComparator
    */
  def generateRecordComparator(name: String): GeneratedRecordComparator = {
    ComparatorCodeGenerator.gen(
        conf,
        name,
        input,
        sortSpec)
  }

  def getter(t: LogicalType, index: Int): String = {
    val prefix = prefixGetFromBinaryRow(t)
    t match {
      case dt: DecimalType =>
        s"get$prefix($index, ${dt.getPrecision}, ${dt.getScale})"
      case dt: TimestampType =>
        s"get$prefix($index, ${dt.getPrecision})"
      case _ =>
        s"get$prefix($index)"
    }
  }

  /**
    * For put${prefix}NormalizedKey() and compare$prefix() of [[SortUtil]].
    */
  def prefixPutNormalizedKey(t: LogicalType): String = prefixGetFromBinaryRow(t)

  /**
    * For get$prefix() of [[org.apache.flink.table.dataformat.TypeGetterSetters]].
    */
  def prefixGetFromBinaryRow(t: LogicalType): String = t.getTypeRoot match {
    case INTEGER => "Int"
    case BIGINT => "Long"
    case SMALLINT => "Short"
    case TINYINT => "Byte"
    case FLOAT => "Float"
    case DOUBLE => "Double"
    case BOOLEAN => "Boolean"
    case VARCHAR | CHAR => "String"
    case VARBINARY | BINARY => "Binary"
    case DECIMAL => "Decimal"
    case DATE => "Int"
    case TIME_WITHOUT_TIME_ZONE => "Int"
    case TIMESTAMP_WITHOUT_TIME_ZONE => "Timestamp"
    case INTERVAL_YEAR_MONTH => "Int"
    case INTERVAL_DAY_TIME => "Long"
    case _ => null
  }

  /**
    * Preventing overflow.
    */
  def safeAddLength(i: Int, j: Int): Int = {
    val sum = i + j
    if (sum < i || sum < j) {
      Integer.MAX_VALUE
    } else {
      sum
    }
  }

  def supportNormalizedKey(t: LogicalType): Boolean = {
    t.getTypeRoot match {
      case _ if PlannerTypeUtils.isPrimitive(t) => true
      case VARCHAR | CHAR | VARBINARY | BINARY |
           DATE | TIME_WITHOUT_TIME_ZONE => true
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        // TODO: support normalize key for non-compact timestamp
        TimestampData.isCompact(t.asInstanceOf[TimestampType].getPrecision)
      case DECIMAL => DecimalData.isCompact(t.asInstanceOf[DecimalType].getPrecision)
      case _ => false
    }
  }

  def getNormalizeKeyLen(t: LogicalType): Int = {
    t.getTypeRoot match {
      case BOOLEAN => 1
      case TINYINT => 1
      case SMALLINT => 2
      case INTEGER => 4
      case FLOAT => 4
      case DOUBLE => 8
      case BIGINT => 8
      case TIMESTAMP_WITHOUT_TIME_ZONE
        if TimestampData.isCompact(t.asInstanceOf[TimestampType].getPrecision) => 8
      case INTERVAL_YEAR_MONTH => 4
      case INTERVAL_DAY_TIME => 8
      case DATE => 4
      case TIME_WITHOUT_TIME_ZONE => 4
      case DECIMAL if DecimalData.isCompact(t.asInstanceOf[DecimalType].getPrecision) => 8
      case VARCHAR | CHAR | VARBINARY | BINARY => Int.MaxValue
    }
  }
}
