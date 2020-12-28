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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.hive.util.TestHiveUDFArray;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFBase64;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link HiveSimpleUDF}. */
public class HiveSimpleUDFTest {
    private static HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());

    @Test
    public void testBooleanUDF() {
        HiveSimpleUDF udf = init(BooleanUDF.class, new DataType[] {DataTypes.INT()});
        assertTrue((boolean) udf.eval(1));
    }

    @Test
    public void testFloatUDF() {
        HiveSimpleUDF udf = init(FloatUDF.class, new DataType[] {DataTypes.FLOAT()});
        assertEquals(3.0f, (float) udf.eval(3.0f), 0);
    }

    @Test
    public void testIntUDF() {
        HiveSimpleUDF udf = init(IntUDF.class, new DataType[] {DataTypes.INT()});
        assertEquals(3, (int) udf.eval(3));
    }

    @Test
    public void testStringUDF() {
        HiveSimpleUDF udf = init(StringUDF.class, new DataType[] {DataTypes.STRING()});
        assertEquals("test", udf.eval("test"));
    }

    @Test
    public void testUDFRand() {
        HiveSimpleUDF udf = init(UDFRand.class, new DataType[0]);

        double result = (double) udf.eval();

        assertTrue(result >= 0 && result < 1);
    }

    @Test
    public void testUDFBin() {
        HiveSimpleUDF udf = init(UDFBin.class, new DataType[] {DataTypes.INT()});

        assertEquals("1100", udf.eval(12));
    }

    @Test
    public void testUDFConv() {
        HiveSimpleUDF udf =
                init(
                        UDFConv.class,
                        new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.INT()});

        assertEquals("1", udf.eval("12", 2, 10));
        assertEquals("-16", udf.eval(-10, 16, -10));
    }

    @Test
    public void testUDFJson() {
        String pattern = "$.owner";
        String json = "{\"store\": \"test\", \"owner\": \"amy\"}";
        String expected = "amy";

        HiveSimpleUDF udf =
                init(UDFJson.class, new DataType[] {DataTypes.STRING(), DataTypes.STRING()});

        assertEquals(expected, udf.eval(json, pattern));

        udf =
                init(
                        UDFJson.class,
                        new DataType[] {DataTypes.CHAR(100), DataTypes.CHAR(pattern.length())});

        assertEquals(expected, udf.eval(json, pattern));

        udf =
                init(
                        UDFJson.class,
                        new DataType[] {
                            DataTypes.VARCHAR(100), DataTypes.VARCHAR(pattern.length())
                        });

        assertEquals(expected, udf.eval(json, pattern));

        // Test invalid CHAR length
        udf =
                init(
                        UDFJson.class,
                        new DataType[] {
                            DataTypes.CHAR(100),
                            DataTypes.CHAR(
                                    pattern.length() - 1) // shorter than pattern's length by 1
                        });

        // Cannot find path "$.owne"
        assertEquals(null, udf.eval(json, pattern));
    }

    @Test
    public void testUDFWeekOfYear() throws FlinkHiveUDFException {
        HiveSimpleUDF udf = init(UDFWeekOfYear.class, new DataType[] {DataTypes.STRING()});

        assertEquals(29, udf.eval("1969-07-20"));
        assertEquals(29, udf.eval(Date.valueOf("1969-07-20")));
        assertEquals(29, udf.eval(Timestamp.valueOf("1969-07-20 00:00:00")));
        assertEquals(1, udf.eval("1980-12-31 12:59:59"));
    }

    @Test
    public void testUDFRegExpExtract() {
        HiveSimpleUDF udf =
                init(
                        UDFRegExpExtract.class,
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()});

        assertEquals("100", udf.eval("100-200", "(\\d+)-(\\d+)", 1));
    }

    @Test
    public void testUDFUnbase64() {
        HiveSimpleUDF udf = init(UDFBase64.class, new DataType[] {DataTypes.BYTES()});

        assertEquals("Cg==", udf.eval(new byte[] {10}));
    }

    @Test
    public void testUDFUnhex() throws UnsupportedEncodingException {
        HiveSimpleUDF udf = init(UDFUnhex.class, new DataType[] {DataTypes.STRING()});

        assertEquals("MySQL", new String((byte[]) udf.eval("4D7953514C"), "UTF-8"));
    }

    @Test
    public void testUDFToInteger() {
        HiveSimpleUDF udf = init(UDFToInteger.class, new DataType[] {DataTypes.DECIMAL(5, 3)});

        assertEquals(1, udf.eval(BigDecimal.valueOf(1.1d)));
    }

    @Test
    public void testUDFArray_singleArray() {
        Double[] testInputs = new Double[] {1.1d, 2.2d};

        // input arg is a single array
        HiveSimpleUDF udf =
                init(TestHiveUDFArray.class, new DataType[] {DataTypes.ARRAY(DataTypes.DOUBLE())});

        assertEquals(3, udf.eval(1.1d, 2.2d));
        assertEquals(3, udf.eval(testInputs));

        // input is not a single array
        udf =
                init(
                        TestHiveUDFArray.class,
                        new DataType[] {DataTypes.INT(), DataTypes.ARRAY(DataTypes.DOUBLE())});

        assertEquals(8, udf.eval(5, testInputs));

        udf =
                init(
                        TestHiveUDFArray.class,
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.ARRAY(DataTypes.DOUBLE()),
                            DataTypes.ARRAY(DataTypes.DOUBLE())
                        });

        assertEquals(11, udf.eval(5, testInputs, testInputs));
    }

    protected static HiveSimpleUDF init(Class hiveUdfClass, DataType[] argTypes) {
        HiveSimpleUDF udf =
                new HiveSimpleUDF(new HiveFunctionWrapper(hiveUdfClass.getName()), hiveShim);

        // Hive UDF won't have literal args
        CallContext callContext = new HiveUDFCallContext(new Object[0], argTypes);
        udf.getTypeInference(null).getOutputTypeStrategy().inferType(callContext);

        udf.open(null);

        return udf;
    }

    /** Boolean Test UDF. */
    public static class BooleanUDF extends UDF {
        public boolean evaluate(int content) {
            return content == 1;
        }
    }

    /** Float Test UDF. */
    public static class FloatUDF extends UDF {
        public float evaluate(float content) {
            return content;
        }
    }

    /** Int Test UDF. */
    public static class IntUDF extends UDF {
        public int evaluate(int content) {
            return content;
        }
    }

    /** String Test UDF. */
    public static class StringUDF extends UDF {
        public String evaluate(String content) {
            return content;
        }
    }

    /** A CallContext implementation for Hive UDF tests. */
    public static class HiveUDFCallContext implements CallContext {

        private final Object[] constantArgs;
        private final DataType[] argTypes;

        public HiveUDFCallContext(Object[] constantArgs, DataType[] argTypes) {
            this.constantArgs = constantArgs;
            this.argTypes = argTypes;
        }

        @Override
        public DataTypeFactory getDataTypeFactory() {
            return null;
        }

        @Override
        public FunctionDefinition getFunctionDefinition() {
            return null;
        }

        @Override
        public boolean isArgumentLiteral(int pos) {
            return pos >= 0 && pos < constantArgs.length && constantArgs[pos] != null;
        }

        @Override
        public boolean isArgumentNull(int pos) {
            return false;
        }

        @Override
        public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
            return (Optional<T>) Optional.of(constantArgs[pos]);
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            return Arrays.asList(argTypes);
        }

        @Override
        public Optional<DataType> getOutputDataType() {
            return Optional.empty();
        }
    }
}
