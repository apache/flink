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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;

/** Test for {@link ArrayDataSerializer}. */
public class ArrayDataSerializerTest {

    /** Test for ArrayData with Primitive data type. */
    public static class BaseArrayWithPrimitiveTest extends SerializerTestBase<ArrayData> {
        @Override
        protected TypeSerializer<ArrayData> createSerializer() {
            return new ArrayDataSerializer(new BigIntType(), LongSerializer.INSTANCE);
        }

        @Override
        protected int getLength() {
            return -1;
        }

        @Override
        protected Class<ArrayData> getTypeClass() {
            return ArrayData.class;
        }

        @Override
        protected ArrayData[] getTestData() {
            return new BinaryArrayData[] {BinaryArrayData.fromPrimitiveArray(new long[] {100L})};
        }
    }

    /** Test for ArrayData with ArrayData data type. */
    public static class ArrayDataWithBinaryArrayTest extends SerializerTestBase<ArrayData> {

        @Override
        protected TypeSerializer<ArrayData> createSerializer() {
            return new ArrayDataSerializer(
                    new ArrayType(new BigIntType()),
                    new ArrayDataSerializer(new BigIntType(), LongSerializer.INSTANCE));
        }

        @Override
        protected int getLength() {
            return -1;
        }

        @Override
        protected Class<ArrayData> getTypeClass() {
            return ArrayData.class;
        }

        @Override
        protected ArrayData[] getTestData() {
            BinaryArrayData elementArray = BinaryArrayData.fromPrimitiveArray(new long[] {100L});
            ArrayDataSerializer elementTypeSerializer =
                    new ArrayDataSerializer(new BigIntType(), LongSerializer.INSTANCE);
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);
            writer.writeArray(0, elementArray, elementTypeSerializer);
            writer.complete();
            return new BinaryArrayData[] {array};
        }
    }

    /** Test for ArrayData with ArrayData data type. */
    public static class BaseArrayWithNullTest extends SerializerTestBase<ArrayData> {

        @Override
        protected TypeSerializer<ArrayData> createSerializer() {
            return new ArrayDataSerializer(new IntType(), IntSerializer.INSTANCE);
        }

        @Override
        protected int getLength() {
            return -1;
        }

        @Override
        protected Class<ArrayData> getTypeClass() {
            return ArrayData.class;
        }

        @Override
        protected ArrayData[] getTestData() {
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
            BinaryArrayWriter.NullSetter nullSetter =
                    BinaryArrayWriter.createNullSetter(new IntType());
            nullSetter.setNull(writer, 0);
            nullSetter.setNull(writer, 1);
            writer.complete();
            return new BinaryArrayData[] {array};
        }
    }
}
