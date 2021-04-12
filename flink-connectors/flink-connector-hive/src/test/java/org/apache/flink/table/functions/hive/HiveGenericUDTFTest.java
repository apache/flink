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

package org.apache.flink.table.functions.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFInline;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFStack;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Test for {@link HiveGenericUDTF}. */
public class HiveGenericUDTFTest {
    private static HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());

    private static TestCollector collector;

    @Test
    public void testOverSumInt() throws Exception {
        Object[] constantArgs = new Object[] {null, 4};

        DataType[] dataTypes = new DataType[] {DataTypes.INT(), DataTypes.INT()};

        HiveGenericUDTF udf = init(TestOverSumIntUDTF.class, constantArgs, dataTypes);

        udf.eval(5, 4);

        assertEquals(Arrays.asList(Row.of(9), Row.of(9)), collector.result);

        // Test empty input and empty output
        constantArgs = new Object[] {};

        dataTypes = new DataType[] {};

        udf = init(TestOverSumIntUDTF.class, constantArgs, dataTypes);

        udf.eval();

        assertEquals(Arrays.asList(), collector.result);
    }

    @Test
    public void testSplit() throws Exception {
        Object[] constantArgs = new Object[] {null};

        DataType[] dataTypes = new DataType[] {DataTypes.STRING()};

        HiveGenericUDTF udf = init(TestSplitUDTF.class, constantArgs, dataTypes);

        udf.eval("1,2,3,5");

        assertEquals(
                Arrays.asList(Row.of("1"), Row.of("2"), Row.of("3"), Row.of("5")),
                collector.result);
    }

    @Test
    public void testStack() throws Exception {
        Object[] constantArgs = new Object[] {2, null, null, null, null};

        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING()
                };

        HiveGenericUDTF udf = init(GenericUDTFStack.class, constantArgs, dataTypes);

        udf.eval(2, "a", "b", "c", "d");

        assertEquals(Arrays.asList(Row.of("a", "b"), Row.of("c", "d")), collector.result);
    }

    @Test
    public void testArray() throws Exception {
        Object[] constantArgs = new Object[] {null};

        DataType[] dataTypes = new DataType[] {DataTypes.ARRAY(DataTypes.INT())};

        HiveGenericUDTF udf = init(GenericUDTFPosExplode.class, constantArgs, dataTypes);

        udf.eval(new Integer[] {1, 2, 3});

        assertEquals(Arrays.asList(Row.of(0, 1), Row.of(1, 2), Row.of(2, 3)), collector.result);
    }

    @Test
    public void testStruct() throws Exception {
        Object[] constantArgs = new Object[] {null};

        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.ARRAY(
                            DataTypes.ROW(
                                    DataTypes.FIELD("1", DataTypes.INT()),
                                    DataTypes.FIELD("2", DataTypes.DOUBLE())))
                };

        HiveGenericUDTF udf = init(GenericUDTFInline.class, constantArgs, dataTypes);

        udf.eval(new Row[] {Row.of(1, 2.2d), Row.of(3, 4.4d)});

        assertEquals(Arrays.asList(Row.of(1, 2.2), Row.of(3, 4.4)), collector.result);
    }

    private static HiveGenericUDTF init(
            Class hiveUdfClass, Object[] constantArgs, DataType[] argTypes) throws Exception {
        HiveFunctionWrapper<GenericUDTF> wrapper = new HiveFunctionWrapper(hiveUdfClass.getName());

        HiveGenericUDTF udf = new HiveGenericUDTF(wrapper, hiveShim);

        udf.setArgumentTypesAndConstants(constantArgs, argTypes);
        udf.getHiveResultType(constantArgs, argTypes);

        ObjectInspector[] argumentInspectors =
                HiveInspectors.toInspectors(hiveShim, constantArgs, argTypes);
        ObjectInspector returnInspector = wrapper.createFunction().initialize(argumentInspectors);

        udf.open(null);

        collector = new TestCollector(returnInspector);
        udf.setCollector(collector);

        return udf;
    }

    private static class TestCollector implements Collector {
        List<Row> result = new ArrayList<>();
        ObjectInspector returnInspector;

        public TestCollector(ObjectInspector returnInspector) {
            this.returnInspector = returnInspector;
        }

        @Override
        public void collect(Object o) {
            Row row = (Row) HiveInspectors.toFlinkObject(returnInspector, o, hiveShim);

            result.add(row);
        }
    }

    /** Test over sum int udtf. */
    public static class TestOverSumIntUDTF extends GenericUDTF {

        ObjectInspectorConverters.Converter[] converters;

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            converters = new ObjectInspectorConverters.Converter[argOIs.length];
            for (int i = 0; i < converters.length; i++) {
                converters[i] =
                        ObjectInspectorConverters.getConverter(
                                argOIs[i], PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            }
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    Collections.singletonList("col1"),
                    Collections.singletonList(
                            PrimitiveObjectInspectorFactory.javaIntObjectInspector));
        }

        @Override
        public void process(Object[] args) throws HiveException {
            int total = 0;
            for (int i = 0; i < args.length; i++) {
                total += (int) converters[i].convert(args[i]);
            }
            for (Object ignored : args) {
                forward(total);
            }
        }

        @Override
        public void close() {}
    }

    /** Test split udtf. */
    public static class TestSplitUDTF extends GenericUDTF {

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    Collections.singletonList("col1"),
                    Collections.singletonList(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        }

        @Override
        public void process(Object[] args) throws HiveException {
            String str = (String) args[0];
            for (String s : str.split(",")) {
                forward(s);
            }
        }

        @Override
        public void close() {}
    }
}
