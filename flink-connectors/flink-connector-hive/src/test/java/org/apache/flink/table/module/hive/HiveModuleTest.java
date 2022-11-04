/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.module.hive;

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link HiveModule}. */
public class HiveModuleTest {

    @Rule public final LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Test
    public void testNumberOfBuiltinFunctions() {
        String hiveVersion = HiveShimLoader.getHiveVersion();
        HiveModule hiveModule = new HiveModule(hiveVersion);
        verifyNumBuiltInFunctions(hiveVersion, hiveModule);

        // creating functions shouldn't change the number of built in functions
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode();
        tableEnv.executeSql("create function myudf as 'org.apache.hadoop.hive.ql.udf.UDFPI'");
        tableEnv.executeSql(
                "create function mygenericudf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'");
        tableEnv.executeSql(
                "create function myudaf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax'");
        tableEnv.executeSql(
                "create function myudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
        verifyNumBuiltInFunctions(hiveVersion, hiveModule);
        // explicitly verify that HiveModule doesn't consider the created functions as built-in
        // functions
        assertThat(hiveModule.getFunctionDefinition("myudf")).isNotPresent();
        assertThat(hiveModule.getFunctionDefinition("mygenericudf")).isNotPresent();
        assertThat(hiveModule.getFunctionDefinition("myudaf")).isNotPresent();
        assertThat(hiveModule.getFunctionDefinition("myudtf")).isNotPresent();
    }

    private void verifyNumBuiltInFunctions(String hiveVersion, HiveModule hiveModule) {
        if (HiveVersionTestUtil.HIVE_310_OR_LATER) {
            assertThat(hiveModule.listFunctions()).hasSize(297);
        } else if (HiveVersionTestUtil.HIVE_230_OR_LATER) {
            assertThat(hiveModule.listFunctions()).hasSize(277);
        } else {
            fail("Unknown test version " + hiveVersion);
        }
    }

    @Test
    public void testHiveBuiltInFunction() {
        FunctionDefinition fd = new HiveModule().getFunctionDefinition("reverse").get();
        HiveSimpleUDF udf = (HiveSimpleUDF) fd;

        DataType[] inputType = new DataType[] {DataTypes.STRING()};

        CallContextMock callContext = new CallContextMock();
        callContext.argumentDataTypes = Arrays.asList(inputType);
        callContext.argumentLiterals = Arrays.asList(new Boolean[inputType.length]);
        Collections.fill(callContext.argumentLiterals, false);
        udf.getTypeInference(null).getOutputTypeStrategy().inferType(callContext);

        udf.open(null);

        assertThat(udf.eval("abc")).isEqualTo("cba");
    }

    @Test
    public void testNonExistFunction() {
        assertThat(new HiveModule().getFunctionDefinition("nonexist")).isNotPresent();
    }

    @Test
    public void testConstantArguments() {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode();

        tEnv.unloadModule("core");
        tEnv.loadModule("hive", new HiveModule());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('an', 'bn')").execute().collect());
        assertThat(results.toString()).isEqualTo("[anbn]");

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('ab', cast('cdefghi' as varchar(5)))")
                                .execute()
                                .collect());
        assertThat(results.toString()).isEqualTo("[abcdefg]");

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('ab',cast(12.34 as decimal(10,5)))")
                                .execute()
                                .collect());
        assertThat(results.toString()).isEqualTo("[ab12.34]");

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        "select concat(cast('2018-01-19' as date),cast('2019-12-27 17:58:23.385' as timestamp))")
                                .execute()
                                .collect());
        assertThat(results.toString()).isEqualTo("[2018-01-192019-12-27 17:58:23.385]");

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('ab',cast(null as int))").execute().collect());
        assertThat(results.toString()).isEqualTo("[null]");
    }

    @Test
    public void testDecimalReturnType() {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode();

        tEnv.unloadModule("core");
        tEnv.loadModule("hive", new HiveModule());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select negative(5.1)").execute().collect());

        assertThat(results.toString()).isEqualTo("[-5.1]");
    }

    @Test
    public void testBlackList() {
        HiveModule hiveModule = new HiveModule();
        assertThat(hiveModule.listFunctions().removeAll(HiveModule.BUILT_IN_FUNC_BLACKLIST))
                .isFalse();
        for (String banned : HiveModule.BUILT_IN_FUNC_BLACKLIST) {
            assertThat(hiveModule.getFunctionDefinition(banned)).isNotPresent();
        }
    }

    @Test
    public void testConstantReturnValue() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select str_to_map('a:1,b:2,c:3',',',':')")
                                .execute()
                                .collect());

        assertThat(results.toString()).isEqualTo("[{a=1, b=2, c=3}]");
    }

    @Test
    public void testEmptyStringLiteralParameters() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());

        // UDF
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select regexp_replace('foobar','oo|ar','')")
                                .execute()
                                .collect());
        assertThat(results.toString()).isEqualTo("[fb]");

        // GenericUDF
        results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select length('')").execute().collect());
        assertThat(results.toString()).isEqualTo("[0]");
    }

    @Test
    public void testFunctionsNeedSessionState() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());
        tableEnv.loadModule("core", CoreModule.INSTANCE);

        tableEnv.sqlQuery("select current_timestamp,current_date").execute().collect();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select mod(-1,2),pmod(-1,2)").execute().collect());
        assertThat(results.toString()).isEqualTo("[-1,1]");
    }

    @Test
    public void testCallUDFWithNoParam() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());
        tableEnv.loadModule("core", CoreModule.INSTANCE);

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select `array`(),`map`()").execute().collect());
        assertThat(results.toString()).isEqualTo("[[],{}]");
    }
}
