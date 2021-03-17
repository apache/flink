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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the type extraction of {@link Writable}. */
@SuppressWarnings("serial")
public class WritableExtractionTest {

    @Test
    public void testDetectWritable() {
        // writable interface itself must not be writable
        assertFalse(TypeExtractor.isHadoopWritable(Writable.class));

        // various forms of extension
        assertTrue(TypeExtractor.isHadoopWritable(DirectWritable.class));
        assertTrue(TypeExtractor.isHadoopWritable(ViaInterfaceExtension.class));
        assertTrue(TypeExtractor.isHadoopWritable(ViaAbstractClassExtension.class));

        // some non-writables
        assertFalse(TypeExtractor.isHadoopWritable(String.class));
        assertFalse(TypeExtractor.isHadoopWritable(List.class));
        assertFalse(TypeExtractor.isHadoopWritable(WritableComparator.class));
    }

    @Test
    public void testCreateWritableInfo() {
        TypeInformation<DirectWritable> info1 =
                TypeExtractor.createHadoopWritableTypeInfo(DirectWritable.class);
        assertEquals(DirectWritable.class, info1.getTypeClass());

        TypeInformation<ViaInterfaceExtension> info2 =
                TypeExtractor.createHadoopWritableTypeInfo(ViaInterfaceExtension.class);
        assertEquals(ViaInterfaceExtension.class, info2.getTypeClass());

        TypeInformation<ViaAbstractClassExtension> info3 =
                TypeExtractor.createHadoopWritableTypeInfo(ViaAbstractClassExtension.class);
        assertEquals(ViaAbstractClassExtension.class, info3.getTypeClass());
    }

    @Test
    public void testValidateTypeInfo() {
        // validate unrelated type info
        TypeExtractor.validateIfWritable(BasicTypeInfo.STRING_TYPE_INFO, String.class);

        // validate writable type info correctly
        TypeExtractor.validateIfWritable(
                new WritableTypeInfo<>(DirectWritable.class), DirectWritable.class);
        TypeExtractor.validateIfWritable(
                new WritableTypeInfo<>(ViaInterfaceExtension.class), ViaInterfaceExtension.class);
        TypeExtractor.validateIfWritable(
                new WritableTypeInfo<>(ViaAbstractClassExtension.class),
                ViaAbstractClassExtension.class);

        // incorrect case: not writable at all
        try {
            TypeExtractor.validateIfWritable(
                    new WritableTypeInfo<>(DirectWritable.class), String.class);
            fail("should have failed with an exception");
        } catch (InvalidTypesException e) {
            // expected
        }

        // incorrect case: wrong writable
        try {
            TypeExtractor.validateIfWritable(
                    new WritableTypeInfo<>(ViaInterfaceExtension.class), DirectWritable.class);
            fail("should have failed with an exception");
        } catch (InvalidTypesException e) {
            // expected
        }
    }

    @Test
    public void testExtractFromFunction() {
        RichMapFunction<DirectWritable, DirectWritable> function =
                new RichMapFunction<DirectWritable, DirectWritable>() {
                    @Override
                    public DirectWritable map(DirectWritable value) throws Exception {
                        return null;
                    }
                };

        TypeInformation<DirectWritable> outType =
                TypeExtractor.getMapReturnTypes(
                        function, new WritableTypeInfo<>(DirectWritable.class));

        assertTrue(outType instanceof WritableTypeInfo);
        assertEquals(DirectWritable.class, outType.getTypeClass());
    }

    @Test
    public void testExtractAsPartOfPojo() {
        PojoTypeInfo<PojoWithWritable> pojoInfo =
                (PojoTypeInfo<PojoWithWritable>) TypeExtractor.getForClass(PojoWithWritable.class);

        boolean foundWritable = false;
        for (int i = 0; i < pojoInfo.getArity(); i++) {
            PojoField field = pojoInfo.getPojoFieldAt(i);
            String name = field.getField().getName();

            if (name.equals("hadoopCitizen")) {
                if (foundWritable) {
                    fail("already seen");
                }
                foundWritable = true;
                assertEquals(
                        new WritableTypeInfo<>(DirectWritable.class), field.getTypeInformation());
                assertEquals(DirectWritable.class, field.getTypeInformation().getTypeClass());
            }
        }

        assertTrue("missed the writable type", foundWritable);
    }

    @Test
    public void testInputValidationError() {

        RichMapFunction<Writable, String> function =
                new RichMapFunction<Writable, String>() {
                    @Override
                    public String map(Writable value) throws Exception {
                        return null;
                    }
                };

        @SuppressWarnings("unchecked")
        TypeInformation<Writable> inType =
                (TypeInformation<Writable>)
                        (TypeInformation<?>) new WritableTypeInfo<>(DirectWritable.class);

        try {
            TypeExtractor.getMapReturnTypes(function, inType);
            fail("exception expected");
        } catch (InvalidTypesException e) {
            // right
        }
    }

    // ------------------------------------------------------------------------
    //  test type classes
    // ------------------------------------------------------------------------

    private interface ExtendedWritable extends Writable {}

    private abstract static class AbstractWritable implements Writable {}

    private static class DirectWritable implements Writable {

        @Override
        public void write(DataOutput dataOutput) throws IOException {}

        @Override
        public void readFields(DataInput dataInput) throws IOException {}
    }

    private static class ViaInterfaceExtension implements ExtendedWritable {

        @Override
        public void write(DataOutput dataOutput) throws IOException {}

        @Override
        public void readFields(DataInput dataInput) throws IOException {}
    }

    private static class ViaAbstractClassExtension extends AbstractWritable {

        @Override
        public void write(DataOutput dataOutput) throws IOException {}

        @Override
        public void readFields(DataInput dataInput) throws IOException {}
    }

    /** Test Pojo containing a {@link DirectWritable}. */
    public static class PojoWithWritable {
        public String str;
        public DirectWritable hadoopCitizen;
    }
}
