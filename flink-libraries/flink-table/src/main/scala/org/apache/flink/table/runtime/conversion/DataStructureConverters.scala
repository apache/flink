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

package org.apache.flink.table.runtime.conversion

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.util
import java.util.{Map => JavaMap}

import javax.annotation.Nullable

import scala.collection.convert.WrapAsJava
import scala.collection.JavaConverters._
import org.apache.commons.codec.binary.Base64
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, _}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BinaryArray.calculateElementSize
import org.apache.flink.table.dataformat._
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions
import org.apache.flink.table.typeutils.TypeUtils._
import org.apache.flink.table.typeutils._
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil

import scala.reflect.ClassTag

/**
 * Functions to convert Scala types to internal types.
 */
object DataStructureConverters {

  def getConverterForType(t: DataType): DataStructureConverter[Any, Any, Any] = {
    val converter = t match {
      case DataTypes.STRING => StringConverter
      case DataTypes.INTERVAL_MILLIS => LongConverter
      case DataTypes.INTERVAL_MONTHS => IntConverter
      case DataTypes.PROCTIME_INDICATOR | DataTypes.ROWTIME_INDICATOR => LongConverter
      case _: TimestampType => TimestampConverter
      case _: DateType => DateConverter
      case DataTypes.TIME => TimeConverter
      case DataTypes.BOOLEAN => BooleanConverter
      case DataTypes.BYTE => ByteConverter
      case DataTypes.SHORT => ShortConverter
      case DataTypes.CHAR => CharConverter
      case DataTypes.INT => IntConverter
      case DataTypes.LONG => LongConverter
      case DataTypes.FLOAT => FloatConverter
      case DataTypes.DOUBLE => DoubleConverter
      case dt: DecimalType => DecimalConverter(dt)
      case DataTypes.BYTE_ARRAY => ByteArrayConverter

      case at: ArrayType if at.getElementType == DataTypes.STRING =>
        StringArrayConverter()
      case at: ArrayType if at.isPrimitive =>
        PrimitiveArrayConverter(at.getElementType)
      case at: ArrayType if isIdentity(at.getElementType) =>
        ClassArrayConverter(at.getElementType)
      case at: ArrayType =>
        ObjectArrayConverter(TypeUtils.getExternalClassForType(at), at.getElementType)

      case mt: MapType if isIdentity(mt.getKeyType) && isIdentity(mt.getValueType) =>
        IdentityMapConverter(mt.getKeyType, mt.getValueType)
      case mt: MapType if (isIdentity(mt.getKeyType) && mt.getValueType == DataTypes.STRING)
        || (isIdentity(mt.getValueType) && mt.getKeyType == DataTypes.STRING) =>
        StringMapConverter(mt.getKeyType, mt.getValueType)
      case mt: MapType =>
        ObjectMapConverter(mt.getKeyType, mt.getValueType)

      case br: RowType => RowConverter(br)
      case gt: GenericType[_] => GenericConverter(gt)

      case tw: TypeInfoWrappedDataType =>
        tw.getTypeInfo match {
          case pa: PrimitiveArrayTypeInfo[_]
            if pa != PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO =>
            PrimitiveArrayConverter(pa.getComponentType)
          case ba: BasicArrayTypeInfo[_, _]
            if ba == BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO =>
            StringArrayConverter()
          case ba: BasicArrayTypeInfo[_, _] =>
            ClassArrayConverter(ba.getComponentInfo)
          case oa: ObjectArrayTypeInfo[_, _] if isIdentity(oa.getComponentInfo) =>
            ClassArrayConverter(oa.getComponentInfo)
          case oa: ObjectArrayTypeInfo[_, _] =>
            ObjectArrayConverter(oa.getTypeClass, oa.getComponentInfo)

          case mt: MapTypeInfo[_, _]
            if isIdentity(mt.getKeyTypeInfo) && isIdentity(mt.getValueTypeInfo) =>
            IdentityMapConverter(mt.getKeyTypeInfo, mt.getValueTypeInfo)
          case mt: MapTypeInfo[_, _]
            if (isIdentity(mt.getKeyTypeInfo)
              && mt.getValueTypeInfo == BasicTypeInfo.STRING_TYPE_INFO)
              || (isIdentity(mt.getValueTypeInfo)
              && mt.getKeyTypeInfo == BasicTypeInfo.STRING_TYPE_INFO) =>
            StringMapConverter(mt.getKeyTypeInfo, mt.getValueTypeInfo)
          case mt: MapTypeInfo[_, _] =>
            ObjectMapConverter(mt.getKeyTypeInfo, mt.getValueTypeInfo)

          case pj: PojoTypeInfo[_] => PojoConverter(pj)
          case tt: TupleTypeInfo[_] => TupleConverter(tt)
          case cc: CaseClassTypeInfo[Product] => CaseClassConverter(cc)
          case _: BinaryStringTypeInfo => BinaryStringConverter
          case d: DecimalTypeInfo => InternalDecimalConverter(d)
          case _ => getConverterForType(tw.toInternalType)
        }
    }
    converter.asInstanceOf[DataStructureConverter[Any, Any, Any]]
  }

  abstract class DataStructureConverter[ScalaInputType, ExternalOutputType, InternalType]
    extends Serializable {

    /**
     * Converts a Scala type to its internal equivalent while automatically handling nulls.
     */
    final def toInternal(@Nullable value: Any): InternalType =
      if (value == null) {
        null.asInstanceOf[InternalType]
      } else {
        toInternalImpl(value.asInstanceOf[ScalaInputType])
      }

    /**
      * Convert a internalType value to its Java/Scala equivalent while
      * automatically handling nulls.
      */
    final def toExternal(@Nullable internalValue: InternalType): ExternalOutputType =
      if (internalValue == null) {
        null.asInstanceOf[ExternalOutputType]
      } else {
        toExternalImpl(internalValue)
      }

    final def toExternal(row: BaseRow, column: Int): ExternalOutputType = {
      if (row.isNullAt(column)) {
        null.asInstanceOf[ExternalOutputType]
      } else {
        toExternalImpl(row, column)
      }
    }

    /**
     * Converts a Scala value to its internal equivalent.
     * @param scalaValue the Scala value, guaranteed not to be null.
     * @return the internal value.
     */
    protected def toInternalImpl(scalaValue: ScalaInputType): InternalType

    /**
      * Convert a internal value to its Java/Scala equivalent.
      */
    def toExternalImpl(internalValue: InternalType): ExternalOutputType

    /**
      * Given a internalType row, convert the value at column `column` to its Java/Scala equivalent.
      * This method will only be called on non-null columns.
      */
    protected def toExternalImpl(row: BaseRow, column: Int): ExternalOutputType
  }

  abstract class AbstractBaseRowConverter[T](numField: Int)
      extends DataStructureConverter[T, T, BaseRow] {
    override def toExternalImpl(row: BaseRow, column: Int): T =
      toExternalImpl(row.getBaseRow(column, numField))
  }

  case class RowConverter(t: RowType)
      extends AbstractBaseRowConverter[Any](t.getArity) {

    private[this] val converters = t.getFieldTypes.map(getConverterForType)

    override def toInternalImpl(row: Any): BaseRow = {
      row match {
        case baseRow: BaseRow => baseRow
        case row: Row =>
          val genericRow = new GenericRow(t.getArity)
          var idx = 0
          while (idx < t.getArity) {
            genericRow.update(idx, converters(idx).toInternal(row.getField(idx)))
            idx += 1
          }
          genericRow
      }
    }

    override def toExternalImpl(baseRow: BaseRow): Any = {
      val row = new Row(baseRow.getArity)
      var idx = 0
      while (idx < baseRow.getArity) {
        row.setField(idx, converters(idx).toExternal(baseRow, idx))
        idx += 1
      }
      row
    }
  }

  case class TupleConverter(t: TupleTypeInfo[_])
      extends AbstractBaseRowConverter[Tuple](t.getArity) {

    private[this] val converters = (0 until t.getArity).map(t.getTypeAt)
        .map(new TypeInfoWrappedDataType(_)).map(getConverterForType)

    override def toInternalImpl(tuple: Tuple): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(tuple.getField(idx)))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Tuple = {
      val tuple = t.getTypeClass.newInstance().asInstanceOf[Tuple]
      var idx = 0
      while (idx < baseRow.getArity) {
        tuple.setField(converters(idx).toExternal(baseRow, idx), idx)
        idx += 1
      }
      tuple
    }
  }

  case class CaseClassConverter(t: CaseClassTypeInfo[Product])
      extends AbstractBaseRowConverter[Product](t.getArity) {

    private[this] val converters = (0 until t.getArity).map(t.getTypeAt)
        .map(new TypeInfoWrappedDataType(_)).map(getConverterForType)
    private val serializer = t.createSerializer(new ExecutionConfig)
        .asInstanceOf[CaseClassSerializer[_]]

    override def toInternalImpl(p: Product): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      val iter = p.productIterator
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(iter.next()))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Product = {
      val fields = new Array[AnyRef](t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        fields(idx) = converters(idx).toExternal(baseRow, idx).asInstanceOf[AnyRef]
        idx += 1
      }
      serializer.createInstance(fields).asInstanceOf[Product]
    }
  }

  case class PojoConverter(t: PojoTypeInfo[_])
      extends AbstractBaseRowConverter[Any](t.getArity) {

    private[this] val converters = (0 until t.getArity).map(t.getTypeAt)
        .map(new TypeInfoWrappedDataType(_)).map(getConverterForType)
    private val fields = (0 until t.getArity).map {idx =>
      val field = t.getPojoFieldAt(idx)
      field.getField.setAccessible(true)
      field
    }

    override def toInternalImpl(pojo: Any): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(
          fields(idx).getField.get(pojo)))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Any = {
      val pojo = t.getTypeClass.newInstance()
      var idx = 0
      while (idx < baseRow.getArity) {
        fields(idx).getField.set(pojo, converters(idx).toExternal(baseRow, idx))
        idx += 1
      }
      pojo
    }
  }

  abstract class IdentityArrayConverter(eleType: DataType, isPrimitive: Boolean)
      extends DataStructureConverter[Any, Any, BaseArray] {

    val internalEleT: InternalType = eleType.toInternalType
    // `isIdentity(eleType)` is true, so the internal type and external type are the same
    val eleClass: Class[_] = TypeUtils.getExternalClassForType(eleType)

    override protected def toInternalImpl(scalaValue: Any): BaseArray =
      scalaValue match {
        case a: Array[_] => new GenericArray(a, a.length, isPrimitive)
        case s: Seq[_] =>
          // FIXME: is there a more efficient way?
          new GenericArray(s.toArray(ClassTag(eleClass)), s.length, isPrimitive)
      }
  }

  case class PrimitiveArrayConverter(eleType: DataType)
      extends IdentityArrayConverter(eleType, true) {

    override def toExternalImpl(internalValue: BaseArray): Any =
      internalValue.toPrimitiveArray(internalEleT)

    override protected def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseArray(column))
  }

  case class ClassArrayConverter(eleType: DataType)
    extends IdentityArrayConverter(eleType, false) {

    override def toExternalImpl(internalValue: BaseArray): Any =
      internalValue.toClassArray(internalEleT, eleClass)

    override protected def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseArray(column))
  }

  case class StringArrayConverter() extends DataStructureConverter[Any, Any, BaseArray] {

    override protected def toInternalImpl(scalaValue: Any): BaseArray = {
      val len = scalaValue match {
        case a: Array[_] => a.length
        case s: Seq[_] => s.length
      }
      val array = new Array[BinaryString](len)

      def setElement(o: Any, i: Int): Unit =
        array(i) = BinaryString.fromString(o)

      scalaValue match {
        case a: Array[_] => a.zipWithIndex.foreach(e => setElement(e._1, e._2))
        case s: Seq[_] => s.zipWithIndex.foreach(e => setElement(e._1, e._2))
      }
      new GenericArray(array, len, false)
    }

    override def toExternalImpl(internalValue: BaseArray): Any = {
      val oa = internalValue.toObjectArray(DataTypes.STRING)
      val array = new Array[String](internalValue.numElements())
      oa.zipWithIndex.foreach(e => array(e._2) =
        if (e._1 == null) null else e._1.toString)
      array
    }

    override protected def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseArray(column))
  }

  case class ObjectArrayConverter(
      externalArrayClass: Class[_],
      eleType: DataType) extends DataStructureConverter[Any, Any, BaseArray] {

    val internalEleT: InternalType = eleType.toInternalType
    val elementConverter: DataStructureConverter[Any, Any, Any] = getConverterForType(eleType)
    val internalEleSer: TypeSerializer[_] = DataTypes.createInternalSerializer(internalEleT)
    private val elementSize = calculateElementSize(internalEleT)

    override protected def toInternalImpl(scalaValue: Any): BaseArray = {
      val len = scalaValue match {
        case a: Array[_] => a.length
        case s: Seq[_] => s.length
      }
      val array = new BinaryArray
      val arrayWriter = new BinaryArrayWriter(array, len, elementSize)

      def writeElement(o: Any, i: Int): Unit =
        if (o == null) {
          arrayWriter.setNullAt(i, internalEleT)
        } else {
          BaseRowUtil.write(
            arrayWriter, i, elementConverter.toInternal(o), internalEleT, internalEleSer)
        }
      scalaValue match {
        case a: Array[_] => a.zipWithIndex.foreach(e => writeElement(e._1, e._2))
        case s: Seq[_] => s.zipWithIndex.foreach(e => writeElement(e._1, e._2))
      }
      arrayWriter.complete()

      array
    }

    override def toExternalImpl(internalValue: BaseArray): Any = {
      val array = internalValue.toObjectArray(internalEleT)
        .map(elementConverter.toExternal)
      if (externalArrayClass eq classOf[Array[AnyRef]]) {
        array
      } else {
        util.Arrays.copyOf(
          array.asInstanceOf[Array[AnyRef]],
          internalValue.numElements(),
          externalArrayClass.asInstanceOf[Class[Array[AnyRef]]])
      }
    }

    override protected def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseArray(column))
  }

  case class IdentityMapConverter(keyType: DataType, valueType: DataType)
      extends DataStructureConverter[Any, Any, BaseMap] {

    private val internalKeyT = keyType.toInternalType
    private val internalValueT = valueType.toInternalType

    override protected def toInternalImpl(scalaValue: Any): BaseMap =
      scalaValue match {
        case m: Map[_, _] => new GenericMap(WrapAsJava.mapAsJavaMap(m))
        case m: JavaMap[_, _] => new GenericMap(m)
      }

    override def toExternalImpl(internalValue: BaseMap): Any = {
      // note that the internalType type and the external type for primitive type data are the same,
      // so we can use `toJavaMap` directly here.
      internalValue.toJavaMap(internalKeyT, internalValueT)
    }

    override protected def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseMap(column))
  }

  case class StringMapConverter(keyType: DataType, valueType: DataType)
      extends DataStructureConverter[Any, Any, BaseMap] {

    private val internalKeyT = keyType.toInternalType
    private val internalValueT = valueType.toInternalType

    override protected def toInternalImpl(scalaValue: Any): BaseMap = {
      val map = new util.HashMap[Any, Any]()

      def putMap(key: Any, value: Any): Unit = (internalKeyT, internalValueT) match {
        case (DataTypes.STRING, _) =>
          map.put(BinaryString.fromString(key), value)
        case (_, DataTypes.STRING) =>
          map.put(key, BinaryString.fromString(value))
      }

      scalaValue match {
        case m: Map[_, _] => for (kv <- m) putMap(kv._1, kv._2)
        case m: JavaMap[_, _] =>
          val iterator = m.entrySet().iterator()
          while (iterator.hasNext) {
            val entry = iterator.next
            putMap(entry.getKey, entry.getValue)
          }
      }
      new GenericMap(map)
    }

    override def toExternalImpl(internalValue: BaseMap): Any = {
      // avoid use scala toMap here, because this may change the order of map values
      val map = new util.HashMap[Any, Any]()

      val javaMap = internalValue.toJavaMap(internalKeyT, internalValueT)
      for (kv <- javaMap.asScala) {
        val (convertedKey, convertedValue) = (internalKeyT, internalValueT) match {
          case (DataTypes.STRING, _) => (if (kv._1 == null) null else kv._1.toString, kv._2)
          case (_, DataTypes.STRING) => (kv._1, if (kv._2 == null) null else kv._2.toString)
        }
        map.put(convertedKey, convertedValue)
      }

      map
    }

    override protected def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseMap(column))
  }

  case class ObjectMapConverter(keyType: DataType, valueType: DataType)
      extends DataStructureConverter[Any, Any, BaseMap] {

    private val internalKeyT = keyType.toInternalType
    private val internalValueT = valueType.toInternalType

    private val internalKeySer = DataTypes.createInternalSerializer(internalKeyT)
    private val internalValueSer = DataTypes.createInternalSerializer(internalValueT)

    private val keyConverter = getConverterForType(keyType)
    private val valueConverter = getConverterForType(valueType)

    private val keyElementSize = calculateElementSize(internalKeyT)
    private val valueElementSize = calculateElementSize(internalValueT)

    private def toBinaryArray(
        a: Array[_],
        elementSize: Int,
        elementType: InternalType,
        elementSerializer: TypeSerializer[_],
        elementConverter: DataStructureConverter[_, _, _]) = {

      val array = new BinaryArray
      val arrayWriter = new BinaryArrayWriter(array, a.length, elementSize)
      a.zipWithIndex.foreach { case (o, index: Int) =>
        if (o == null) {
          arrayWriter.setNullAt(index, elementType)
        } else {
          BaseRowUtil.write(
            arrayWriter, index, elementConverter.toInternal(o), elementType, elementSerializer)
        }
      }
      arrayWriter.complete()
      array
    }

    override def toInternalImpl(scalaValue: Any): BaseMap = {
      val (keys, values) = scalaValue match {
        case map: Map[_, _] =>
          val keys: Array[Any] = new Array[Any](map.size)
          val values: Array[Any] = new Array[Any](map.size)

          var i = 0
          for ((key, value) <- map.iterator) {
            keys(i) = key
            values(i) = value
            i += 1
          }
          (keys, values)
        case javaMap: JavaMap[_, _] =>
          val keys: Array[Any] = new Array[Any](javaMap.size())
          val values: Array[Any] = new Array[Any](javaMap.size())

          var i: Int = 0
          val iterator = javaMap.entrySet().iterator()
          while (iterator.hasNext) {
            val entry = iterator.next()
            keys(i) = entry.getKey
            values(i) = entry.getValue
            i += 1
          }
          (keys, values)
      }

      BinaryMap.valueOf(
        toBinaryArray(keys, keyElementSize, internalKeyT, internalKeySer, keyConverter),
        toBinaryArray(values, valueElementSize, internalValueT, internalValueSer, valueConverter))
    }

    override def toExternalImpl(internalValue: BaseMap): Any = {
      // avoid use scala toMap here, because this may change the order of map values
      val map = new util.HashMap[Any, Any]()

      val javaMap = internalValue.toJavaMap(internalKeyT, internalValueT)
      for (kv <- javaMap.asScala) {
        val convertedKey = keyConverter.toExternal(kv._1)
        val convertedValue = valueConverter.toExternal(kv._2)
        map.put(convertedKey, convertedValue)
      }

      map
    }

    override def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getBaseMap(column))
  }

  object StringConverter extends DataStructureConverter[Any, String, BinaryString] {
    override def toInternalImpl(scalaValue: Any): BinaryString = scalaValue match {
      case str: String => BinaryString.fromString(str)
      case utf8: BinaryString => utf8
    }
    override def toExternalImpl(internalValue: BinaryString): String = internalValue.toString
    override def toExternalImpl(row: BaseRow, column: Int): String =
      row.getBinaryString(column).toString
  }

  object TimestampConverter extends DataStructureConverter[Timestamp, Timestamp, Any] {
    override def toInternalImpl(scalaValue: Timestamp): Long =
      BuildInScalarFunctions.toLong(scalaValue)
    override def toExternalImpl(internalValue: Any): Timestamp =
      BuildInScalarFunctions.internalToTimestamp(internalValue.asInstanceOf[Long])
    override def toExternalImpl(row: BaseRow, column: Int): Timestamp =
      BuildInScalarFunctions.internalToTimestamp(row.getLong(column))
  }

  object DateConverter extends DataStructureConverter[Date, Date, Any] {
    override def toInternalImpl(scalaValue: Date): Int = BuildInScalarFunctions.toInt(scalaValue)
    override def toExternalImpl(internalValue: Any): Date =
      BuildInScalarFunctions.internalToDate(internalValue.asInstanceOf[Int])
    override def toExternalImpl(row: BaseRow, column: Int): Date =
      BuildInScalarFunctions.internalToDate(row.getInt(column))
  }

  object TimeConverter extends DataStructureConverter[Time, Time, Any] {
    override def toInternalImpl(scalaValue: Time): Int = BuildInScalarFunctions.toInt(scalaValue)
    override def toExternalImpl(internalValue: Any): Time =
      BuildInScalarFunctions.internalToTime(internalValue.asInstanceOf[Int])
    override def toExternalImpl(row: BaseRow, column: Int): Time =
      BuildInScalarFunctions.internalToTime(row.getInt(column))
  }

  abstract class IdentityConverter[T] extends DataStructureConverter[T, Any, Any] {
    final override def toExternalImpl(internalValue: Any): Any = internalValue
    final override def toInternalImpl(scalaValue: T): Any = scalaValue
  }

  object BooleanConverter extends IdentityConverter[Boolean] {
    override def toExternalImpl(row: BaseRow, column: Int): Boolean = row.getBoolean(column)
  }

  object ByteConverter extends IdentityConverter[Byte] {
    override def toExternalImpl(row: BaseRow, column: Int): Byte = row.getByte(column)
  }

  object ShortConverter extends IdentityConverter[Short] {
    override def toExternalImpl(row: BaseRow, column: Int): Short = row.getShort(column)
  }

  object CharConverter extends IdentityConverter[Character] {
    override def toExternalImpl(row: BaseRow, column: Int): Character = row.getChar(column)
  }

  object IntConverter extends IdentityConverter[Int] {
    override def toExternalImpl(row: BaseRow, column: Int): Int = row.getInt(column)
  }

  object LongConverter extends IdentityConverter[Long] {
    override def toExternalImpl(row: BaseRow, column: Int): Long = row.getLong(column)
  }

  object FloatConverter extends IdentityConverter[Float] {
    override def toExternalImpl(row: BaseRow, column: Int): Float = row.getFloat(column)
  }

  object DoubleConverter extends IdentityConverter[Double] {
    override def toExternalImpl(row: BaseRow, column: Int): Double = row.getDouble(column)
  }

  object ByteArrayConverter extends IdentityConverter[Array[Byte]] {
    override def toExternalImpl(row: BaseRow, column: Int): Array[Byte] = row.getByteArray(column)
  }

  object BinaryStringConverter extends IdentityConverter[BinaryString] {
    override def toExternalImpl(row: BaseRow, column: Int): BinaryString =
    row.getBinaryString(column)
  }

  case class InternalDecimalConverter(dt: DecimalTypeInfo) extends IdentityConverter[Decimal] {
    override def toExternalImpl(row: BaseRow, column: Int): Decimal =
      row.getDecimal(column, dt.precision(), dt.scale())
  }

  case class DecimalConverter(dt: DecimalType)
    extends DataStructureConverter[Any, JBigDecimal, Decimal] {
    override def toInternalImpl(scalaValue: Any): Decimal = scalaValue match {
      case bd: JBigDecimal => Decimal.fromBigDecimal(bd, dt.precision(), dt.scale())
      case sd: Decimal => sd
    }
    override def toExternalImpl(internalValue: Decimal): JBigDecimal =
      internalValue.toBigDecimal
    override def toExternalImpl(row: BaseRow, column: Int): JBigDecimal =
      row.getDecimal(column, dt.precision(), dt.scale()).toBigDecimal
  }

  case class GenericConverter(t: GenericType[_]) extends IdentityConverter[Any] {
    override def toExternalImpl(row: BaseRow, column: Int): Any = row.getGeneric(column, t)
  }

  def createToInternalConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      identity
    } else {
      getConverterForType(dataType).toInternal
    }
  }

  /**
   * Creates a converter function that will convert internal types to Java/Scala type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  def createToExternalConverter(t: DataType): Any => Any = {
    if (isPrimitive(t)) {
      identity
    } else {
      getConverterForType(t).toExternal
    }
  }

  private def genConvertField(ctx: CodeGeneratorContext, converter: Any => Any): String = {
    val convertField = CodeGenUtils.newName("converter")
    val convertTypeTerm = classOf[(_) => _].getCanonicalName

    val initCode = if (ctx.supportReference) {
      s"$convertField = ${ctx.addReferenceObj(converter)};"
    } else {
      val byteArray = InstantiationUtil.serializeObject(converter)
      val serializedData = Base64.encodeBase64URLSafeString(byteArray)
      s"""
         | $convertField = ($convertTypeTerm)
         |      ${classOf[InstantiationUtil].getCanonicalName}.deserializeObject(
         |         ${classOf[Base64].getCanonicalName}.decodeBase64("$serializedData"),
         |         Thread.currentThread().getContextClassLoader());
           """.stripMargin
    }

    ctx.addReusableMember(s"$convertTypeTerm $convertField = null;", initCode)
    convertField
  }

  def genToExternal(ctx: CodeGeneratorContext, t: DataType, term: String): String = {
    val iTerm = boxedTypeTermForType(t.toInternalType)
    val eTerm = externalBoxedTermForType(t)
    if (isIdentity(t)) {
      s"($eTerm) $term"
    } else {
      val scalarFuncTerm = classOf[BuildInScalarFunctions].getCanonicalName
      TypeConverters.createExternalTypeInfoFromDataType(t) match {
        case Types.STRING => s"$BINARY_STRING.safeToString(($iTerm) $term)"
        case Types.SQL_DATE => s"$scalarFuncTerm.safeInternalToDate(($iTerm) $term)"
        case Types.SQL_TIME => s"$scalarFuncTerm.safeInternalToTime(($iTerm) $term)"
        case Types.SQL_TIMESTAMP => s"$scalarFuncTerm.safeInternalToTimestamp(($iTerm) $term)"
        case _ =>
          s"($eTerm) ${genConvertField(ctx, createToExternalConverter(t))}.apply($term)"
      }
    }
  }

  def genToInternal(ctx: CodeGeneratorContext, t: DataType, term: String): String =
    genToInternal(ctx, t)(term)

  def genToInternal(ctx: CodeGeneratorContext, t: DataType): String => String = {
    val iTerm = boxedTypeTermForType(t.toInternalType)
    val eTerm = externalBoxedTermForType(t)
    if (isIdentity(t)) {
      term => s"($iTerm) $term"
    } else {
      val scalarFuncTerm = classOf[BuildInScalarFunctions].getCanonicalName
      TypeConverters.createExternalTypeInfoFromDataType(t) match {
        case Types.STRING => term => s"$BINARY_STRING.fromString($term)"
        case Types.SQL_DATE | Types.SQL_TIME =>
          term => s"$scalarFuncTerm.safeToInt(($eTerm) $term)"
        case Types.SQL_TIMESTAMP => term => s"$scalarFuncTerm.safeToLong(($eTerm) $term)"
        case _ =>
          val converter = genConvertField(ctx, createToInternalConverter(t))
          term => s"($iTerm) $converter.apply($term)"
      }
    }
  }

  private def isIdentity(t: DataType): Boolean = {
    isPrimitive(t) || getConverterForType(t).isInstanceOf[IdentityConverter[_]]
  }

  def genToExternalIfNeeded(
      ctx: CodeGeneratorContext,
      t: DataType,
      clazz: Class[_],
      term: String): String = {
    if (isInternalClass(clazz, t.toInternalType)) {
      s"(${boxedTypeTermForType(t.toInternalType)}) $term"
    } else {
      genToExternal(ctx, t, term)
    }
  }

  def genToInternalIfNeeded(
      ctx: CodeGeneratorContext,
      t: DataType,
      clazz: Class[_],
      term: String): String = {
    if (isInternalClass(clazz, t.toInternalType)) {
      s"(${boxedTypeTermForType(t.toInternalType)}) $term"
    } else {
      genToInternal(ctx, t, term)
    }
  }
}
