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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.table.api.types._
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.CodeGeneratorContext.BASE_ROW
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.dataformat.util.BinaryRowUtil
import org.apache.flink.table.runtime.sort.{NormalizedKeyComputer, RecordComparator}

import scala.collection.mutable

/**
  * A code generator for generating [[NormalizedKeyComputer]] and [[RecordComparator]].
  *
  * @param keys        key positions describe which fields are keys in what order.
  * @param keyTypes       types for the key fields, in the same order as the key fields.
  * @param keyComparators comparators for the key fields.
  * @param orders      sorting orders for the key fields.
  * @param nullsIsLast      Ordering of nulls.
  */
class SortCodeGenerator(
    val keys: Array[Int],
    val keyTypes: Array[InternalType],
    val keyComparators: Array[TypeComparator[_ <: Any]],
    val orders: Array[Boolean],
    val nullsIsLast: Array[Boolean]) {

  val binaryRowUtil = "org.apache.flink.table.dataformat.util.BinaryRowUtil"
  val memorySegment = "org.apache.flink.core.memory.MemorySegment"

  /** Chunks for long, int, short, byte */
  val POSSIBLE_CHUNK_SIZES = Array(8, 4, 2, 1)

  /** For get${operator} set${operator} of [[org.apache.flink.core.memory.MemorySegment]] */
  val BYTE_OPERATOR_MAPPING = Map(8 -> "Long", 4 -> "Int", 2 -> "Short", 1 -> "")

  /** For primitive define */
  val BYTE_DEFINE_MAPPING = Map(8 -> "long", 4 -> "int", 2 -> "short", 1 -> "byte")

  /** For Class of primitive type */
  val BYTE_CLASS_MAPPING = Map(8 -> "Long", 4 -> "Integer", 2 -> "Short", 1 -> "Byte")

  /** Normalized meta */
  val (nullAwareNormalizedKeyLen, normalizedKeyNum, invertNormalizedKey, normalizedKeyLengths) = {
    var keyLen = 0
    var keyNum = 0
    var inverted = false
    val keyLengths = new mutable.ArrayBuffer[Int]
    var break = false
    var i = 0
    while (i < keys.length && !break) {
      val t = keyTypes(i)
      val comparator = keyComparators(i)
      if (supportNormalizedKey(t, comparator)) {
        val invert = comparator.invertNormalizedKey

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
          val len = safeAddLength(getNormalizeKeyLen(t, comparator), 1)
          if (len < 0) {
            throw new RuntimeException("Comparator " + comparator +
                " specifies an invalid length for the normalized key: " + len)
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
      (false, Math.min(SortCodeGenerator.MAX_NORMALIZED_KEY_LEN, 8 * normalizedKeyNum))
    } else {
      (normalizedKeyNum == keys.length, nullAwareNormalizedKeyLen)
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
      public class $className extends ${baseClass.getCanonicalName} {

        @Override
        public void putKey($BASE_ROW record, $memorySegment target, int offset) {
          ${putKeys.mkString}
          ${reverseKeys.mkString}
        }

        @Override
        public int compareKey($memorySegment segI, int offsetI, $memorySegment segJ, int offsetJ) {
          ${compareKeys.mkString}
        }

        @Override
        public void swapKey($memorySegment segI, int offsetI, $memorySegment segJ, int offsetJ) {
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

    GeneratedNormalizedKeyComputer(className, code)
  }

  def generatePutNormalizedKeys(numKeyBytes: Int): mutable.ArrayBuffer[String] = {
    /* Example generated code, for int:
    if (record.isNullAt(0)) {
      org.apache.flink.table.dataformat.util.BinaryRowUtil.minNormalizedKey(target, offset+0, 5);
    } else {
      target.put(offset+0, (byte) 1);
      org.apache.flink.table.dataformat.util.BinaryRowUtil.putIntNormalizedKey(
        record.getInt(0), target, offset+1, 4);
    }
     */
    val putKeys = new mutable.ArrayBuffer[String]
    var bytesLeft = numKeyBytes
    var currentOffset = 0
    var keyIndex = 0
    while (bytesLeft > 0 && keyIndex < normalizedKeyNum) {
      var len = normalizedKeyLengths(keyIndex)
      val index = keys(keyIndex)
      val nullIsMaxValue = orders(keyIndex) == nullsIsLast(keyIndex)
      len = if (bytesLeft >= len) len else bytesLeft
      val t = keyTypes(keyIndex)
      val prefix = prefixGetFromBinaryRow(t)
      val putCode = {
        if (prefix != null) {
          t match {
            case _ if BinaryRow.isMutable(t) =>
              val get = getter(t, index)
              s"""
                 |target.put(offset+$currentOffset, (byte) 1);
                 |$binaryRowUtil.put${prefixPutKey(t)}NormalizedKey(
                 |  record.$get, target, offset+${currentOffset + 1}, ${len - 1});
                 |
             """.stripMargin
            case _ =>
              // It is BinaryString/byte[], we can omit the null aware byte(zero is the smallest),
              // because there is no other field behind, and is not keyFullyDetermines.
              s"""
                 |$binaryRowUtil.put${prefixPutKey(t)}NormalizedKey(
                 |  record.get$prefix($index), target, offset+$currentOffset, $len);
                 |""".stripMargin
          }
        } else {
          s"""
             |target.put(offset+$currentOffset, (byte) 1);
             |comparators[$keyIndex].putNormalizedKey(
             |  record.getGeneric($index, serializers[$keyIndex]),
             |  target, offset+${currentOffset + 1}, ${len - 1});
             |""".stripMargin
        }
      }
      val nullCode = if (nullIsMaxValue) {
        s"$binaryRowUtil.maxNormalizedKey(target, offset+$currentOffset, $len);"
      } else {
        s"$binaryRowUtil.minNormalizedKey(target, offset+$currentOffset, $len);"
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
    if (BinaryRow.LITTLE_ENDIAN) {
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

    /* Example generated code, for int:
    boolean null0At1 = o1.isNullAt(0);
    boolean null0At2 = o2.isNullAt(0);
    int cmp0 = null0At1 && null0At2 ? 0 :
      (null0At1 ? -1 :
        (null0At2 ? 1 : BinaryRowUtil.compareInt(o1.getInt(0), o2.getInt(0))));
    if (cmp0 != 0) {
      return cmp0;
    }
    return 0;
     */

    val className = newName(name)

    val compares = new mutable.ArrayBuffer[String]

    for (i <- keys.indices) {
      val index = keys(i)

      val symbol = if (orders(i)) "" else "-"

      val nullIsLast = if (nullsIsLast(i)) 1 else -1

      val t = keyTypes(i)

      val prefix = prefixGetFromBinaryRow(t)

      val compCompare = s"comparators[$i].compare"

      val compareCode = if (prefix != null) {
        t match {
          case bt: RowType =>
            val arity = bt.getArity
            s"$compCompare(o1.get$prefix($index, $arity), o2.get$prefix($index, $arity))"
          case _ =>
            val get = getter(t, index)
            s"$symbol$binaryRowUtil.compare$prefix(o1.$get, o2.$get)"
        }
      } else {
        // Only builtIn keys need care about invertNormalizedKey(order).
        // Because comparators will handle order.
        s"""
           |$compCompare(
           |  o1.getGeneric($index, serializers[$i]),
           |  o2.getGeneric($index, serializers[$i]))
           |""".stripMargin
      }

      val code =
        s"""
           |boolean null${index}At1 = o1.isNullAt($index);
           |boolean null${index}At2 = o2.isNullAt($index);
           |int cmp$index = null${index}At1 && null${index}At2 ? 0 :
           |  (null${index}At1 ? $nullIsLast :
           |    (null${index}At2 ? ${-nullIsLast} : $compareCode));
           |if (cmp$index != 0) {
           |  return cmp$index;
           |}
           |""".stripMargin
      compares += code
    }
    compares += "return 0;"

    val baseClass = classOf[RecordComparator]

    val code =
      j"""
      public class $className extends ${baseClass.getCanonicalName} {

        @Override
        public int compare($BASE_ROW o1, $BASE_ROW o2) {
          ${compares.mkString}
        }

      }
      """.stripMargin

    GeneratedRecordComparator(className, code)
  }

  def getter(t: InternalType, index: Int): String = {
    val prefix = prefixGetFromBinaryRow(t)
    t match {
      case dt: DecimalType =>
        s"get$prefix($index, ${dt.precision()}, ${dt.scale()})"
      case _ =>
        s"get$prefix($index)"
    }
  }

  def prefixPutKey(t: InternalType): String = prefixGetFromBinaryRow(t)

  /**
    * For compare$prefix() and put${prefix}NormalizedKey() of
    * [[BinaryRowUtil]].
    */
  def prefixGetFromBinaryRow(t: InternalType): String = t match {
    case DataTypes.INT => "Int"
    case DataTypes.LONG => "Long"
    case DataTypes.SHORT => "Short"
    case DataTypes.BYTE => "Byte"
    case DataTypes.FLOAT => "Float"
    case DataTypes.DOUBLE => "Double"
    case DataTypes.BOOLEAN => "Boolean"
    case DataTypes.CHAR => "Char"
    case DataTypes.STRING => "BinaryString"
    case _: DecimalType => "Decimal"
    case DataTypes.BYTE_ARRAY => "ByteArray"
    case _: DateType => "Int"
    case DataTypes.TIME => "Int"
    case _: TimestampType => "Long"
    case _: RowType => "BaseRow"
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

  def supportNormalizedKey(t: InternalType, comparator: TypeComparator[_]): Boolean = {
    t match {
      case DataTypes.BYTE_ARRAY | DataTypes.FLOAT | DataTypes.DOUBLE => true
      case _: ArrayType | _: MapType | _: RowType => false
      case _ => comparator.supportsNormalizedKey
    }
  }

  def getNormalizeKeyLen(t: InternalType, comparator: TypeComparator[_]): Int = {
    t match {
      case DataTypes.BYTE_ARRAY => Integer.MAX_VALUE
      case DataTypes.FLOAT => 4
      case DataTypes.DOUBLE => 8
      case _ => comparator.getNormalizeKeyLen
    }
  }
}

object SortCodeGenerator{
  val MAX_NORMALIZED_KEY_LEN = 16
}
