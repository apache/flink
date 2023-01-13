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
import org.apache.flink.table.functions.hive.util.TestHiveUDFArray;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;

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
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveSimpleUDF}. */
public class HiveSimpleUDFTest {
    private static HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());

    @Test
    public void testBooleanUDF() {
        HiveSimpleUDF udf = init(BooleanUDF.class, new DataType[] {DataTypes.INT()});
        assertThat((boolean) udf.eval(1)).isTrue();
    }

    @Test
    public void testFloatUDF() {
        HiveSimpleUDF udf = init(FloatUDF.class, new DataType[] {DataTypes.FLOAT()});
        assertThat((float) udf.eval(3.0f)).isEqualTo(3.0f);
    }

    @Test
    public void testIntUDF() {
        HiveSimpleUDF udf = init(IntUDF.class, new DataType[] {DataTypes.INT()});
        assertThat((int) udf.eval(3)).isEqualTo(3);
    }

    @Test
    public void testStringUDF() {
        HiveSimpleUDF udf = init(StringUDF.class, new DataType[] {DataTypes.STRING()});
        assertThat(udf.eval("test")).isEqualTo("test");
    }

    @Test
    public void testUDFRand() {
        HiveSimpleUDF udf = init(UDFRand.class, new DataType[0]);

        double result = (double) udf.eval();

        assertThat(result >= 0 && result < 1).isTrue();
    }

    @Test
    public void testUDFBin() {
        HiveSimpleUDF udf = init(UDFBin.class, new DataType[] {DataTypes.INT()});

        assertThat(udf.eval(12)).isEqualTo("1100");
    }

    @Test
    public void testUDFConv() {
        HiveSimpleUDF udf =
                init(
                        UDFConv.class,
                        new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.INT()});

        assertThat(udf.eval("12", 2, 10)).isEqualTo("1");
        assertThat(udf.eval(-10, 16, -10)).isEqualTo("-16");
    }

    @Test
    public void testUDFJson() {
        String pattern = "$.owner";
        String json = "{\"store\": \"test\", \"owner\": \"amy\"}";
        String expected = "amy";

        HiveSimpleUDF udf =
                init(UDFJson.class, new DataType[] {DataTypes.STRING(), DataTypes.STRING()});

        assertThat(udf.eval(json, pattern)).isEqualTo(expected);

        udf =
                init(
                        UDFJson.class,
                        new DataType[] {DataTypes.CHAR(100), DataTypes.CHAR(pattern.length())});

        assertThat(udf.eval(json, pattern)).isEqualTo(expected);

        udf =
                init(
                        UDFJson.class,
                        new DataType[] {
                            DataTypes.VARCHAR(100), DataTypes.VARCHAR(pattern.length())
                        });

        assertThat(udf.eval(json, pattern)).isEqualTo(expected);

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
        assertThat(udf.eval(json, pattern)).isNull();
    }

    @Test
    public void testUDFWeekOfYear() throws FlinkHiveUDFException {
        HiveSimpleUDF udf = init(UDFWeekOfYear.class, new DataType[] {DataTypes.STRING()});

        assertThat(udf.eval("1969-07-20")).isEqualTo(29);
        assertThat(udf.eval(Date.valueOf("1969-07-20"))).isEqualTo(29);
        assertThat(udf.eval(Timestamp.valueOf("1969-07-20 00:00:00"))).isEqualTo(29);
        assertThat(udf.eval("1980-12-31 12:59:59")).isEqualTo(1);
    }

    @Test
    public void testUDFRegExpExtract() {
        HiveSimpleUDF udf =
                init(
                        UDFRegExpExtract.class,
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()});

        assertThat(udf.eval("100-200", "(\\d+)-(\\d+)", 1)).isEqualTo("100");
    }

    @Test
    public void testUDFUnbase64() {
        HiveSimpleUDF udf = init(UDFBase64.class, new DataType[] {DataTypes.BYTES()});

        assertThat(udf.eval(new byte[] {10})).isEqualTo("Cg==");
    }

    @Test
    public void testUDFUnhex() throws UnsupportedEncodingException {
        HiveSimpleUDF udf = init(UDFUnhex.class, new DataType[] {DataTypes.STRING()});

        assertThat(new String((byte[]) udf.eval("4D7953514C"), "UTF-8")).isEqualTo("MySQL");
    }

    @Test
    public void testUDFToInteger() {
        HiveSimpleUDF udf = init(UDFToInteger.class, new DataType[] {DataTypes.DECIMAL(5, 3)});

        assertThat(udf.eval(BigDecimal.valueOf(1.1d))).isEqualTo(1);
    }

    @Test
    public void testUDFArray_singleArray() {
        Double[] testInputs = new Double[] {1.1d, 2.2d};

        // input arg is a single array
        HiveSimpleUDF udf =
                init(TestHiveUDFArray.class, new DataType[] {DataTypes.ARRAY(DataTypes.DOUBLE())});

        assertThat(udf.eval(1.1d, 2.2d)).isEqualTo(3);
        assertThat(udf.eval(testInputs)).isEqualTo(3);

        // input is not a single array
        udf =
                init(
                        TestHiveUDFArray.class,
                        new DataType[] {DataTypes.INT(), DataTypes.ARRAY(DataTypes.DOUBLE())});

        assertThat(udf.eval(5, testInputs)).isEqualTo(8);

        udf =
                init(
                        TestHiveUDFArray.class,
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.ARRAY(DataTypes.DOUBLE()),
                            DataTypes.ARRAY(DataTypes.DOUBLE())
                        });

        assertThat(udf.eval(5, testInputs, testInputs)).isEqualTo(11);
    }

    protected static HiveSimpleUDF init(Class<?> hiveUdfClass, DataType[] argTypes) {
        HiveSimpleUDF udf = new HiveSimpleUDF(new HiveFunctionWrapper<>(hiveUdfClass), hiveShim);

        // Hive UDF won't have literal args
        CallContextMock callContext = new CallContextMock();
        callContext.argumentDataTypes = Arrays.asList(argTypes);
        callContext.argumentLiterals = Arrays.asList(new Boolean[argTypes.length]);
        Collections.fill(callContext.argumentLiterals, false);
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
}
