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
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveGenericUDAF}. */
public class HiveGenericUDAFTest {
    @Test
    public void testUDAFMin() throws Exception {
        Object[] constantArgs = new Object[] {null};

        DataType[] argTypes = new DataType[] {DataTypes.BIGINT()};

        HiveGenericUDAF udf = init(GenericUDAFMin.class, constantArgs, argTypes);

        GenericUDAFEvaluator.AggregationBuffer acc = udf.createAccumulator();

        udf.accumulate(acc, 2L);
        udf.accumulate(acc, 3L);
        udf.accumulate(acc, 1L);

        udf.merge(acc, Collections.emptyList());

        assertThat(udf.getValue(acc)).isEqualTo(1L);
    }

    @Test
    public void testUDAFSum() throws Exception {
        Object[] constantArgs = new Object[] {null};

        DataType[] argTypes = new DataType[] {DataTypes.DOUBLE()};

        HiveGenericUDAF udf = init(GenericUDAFSum.class, constantArgs, argTypes);

        GenericUDAFEvaluator.AggregationBuffer acc = udf.createAccumulator();

        udf.accumulate(acc, 0.5d);
        udf.accumulate(acc, 0.3d);
        udf.accumulate(acc, 5.3d);

        udf.merge(acc, Collections.emptyList());

        assertThat(udf.getValue(acc)).isEqualTo(6.1d);

        constantArgs = new Object[] {null};

        argTypes = new DataType[] {DataTypes.DECIMAL(5, 3)};

        udf = init(GenericUDAFSum.class, constantArgs, argTypes);

        acc = udf.createAccumulator();

        udf.accumulate(acc, BigDecimal.valueOf(10.111));
        udf.accumulate(acc, BigDecimal.valueOf(3.222));
        udf.accumulate(acc, BigDecimal.valueOf(5.333));

        udf.merge(acc, Collections.emptyList());

        assertThat(udf.getValue(acc)).isEqualTo(BigDecimal.valueOf(18.666));
    }

    @Test
    public void testUDAFCount() throws Exception {
        Object[] constantArgs = new Object[] {null};

        DataType[] argTypes = new DataType[] {DataTypes.DOUBLE()};

        HiveGenericUDAF udf = init(GenericUDAFCount.class, constantArgs, argTypes);

        GenericUDAFEvaluator.AggregationBuffer acc = udf.createAccumulator();

        udf.accumulate(acc, 0.5d);
        udf.accumulate(acc, 0.3d);
        udf.accumulate(acc, 5.3d);

        udf.merge(acc, Collections.emptyList());

        assertThat(udf.getValue(acc)).isEqualTo(3L);
    }

    private static HiveGenericUDAF init(
            Class<?> hiveUdfClass, Object[] constantArgs, DataType[] argTypes) throws Exception {
        HiveFunctionWrapper<GenericUDAFResolver> wrapper = new HiveFunctionWrapper<>(hiveUdfClass);

        CallContextMock callContext = new CallContextMock();
        callContext.argumentDataTypes = Arrays.asList(argTypes);
        callContext.argumentValues =
                Arrays.stream(constantArgs).map(Optional::ofNullable).collect(Collectors.toList());
        callContext.argumentLiterals =
                Arrays.stream(constantArgs).map(Objects::nonNull).collect(Collectors.toList());

        HiveGenericUDAF udf =
                new HiveGenericUDAF(
                        wrapper, HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion()));

        udf.setArguments(callContext);
        udf.inferReturnType();

        udf.open(null);

        return udf;
    }
}
