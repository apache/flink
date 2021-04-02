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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.table.functions.hive.HiveSimpleUDFTest.HiveUDFCallContext;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_0_1;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_1_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_2_1;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_0_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_1_1;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_2_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_3_4;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V3_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/** Test for {@link HiveModule}. */
public class HiveModuleTest {

    @Rule public final LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Test
    public void testNumberOfBuiltinFunctions() {
        String hiveVersion = HiveShimLoader.getHiveVersion();
        HiveModule hiveModule = new HiveModule(hiveVersion);
        verifyNumBuiltInFunctions(hiveVersion, hiveModule);

        // creating functions shouldn't change the number of built in functions
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
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
        assertFalse(hiveModule.getFunctionDefinition("myudf").isPresent());
        assertFalse(hiveModule.getFunctionDefinition("mygenericudf").isPresent());
        assertFalse(hiveModule.getFunctionDefinition("myudaf").isPresent());
        assertFalse(hiveModule.getFunctionDefinition("myudtf").isPresent());
    }

    private void verifyNumBuiltInFunctions(String hiveVersion, HiveModule hiveModule) {
        switch (hiveVersion) {
            case HIVE_VERSION_V1_0_1:
                assertEquals(195, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V1_1_0:
                assertEquals(200, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V1_2_1:
                assertEquals(220, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V2_0_0:
                assertEquals(231, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V2_1_1:
                assertEquals(241, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V2_2_0:
                assertEquals(256, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V2_3_4:
                assertEquals(274, hiveModule.listFunctions().size());
                break;
            case HIVE_VERSION_V3_1_1:
                assertEquals(293, hiveModule.listFunctions().size());
                break;
            default:
                fail("Unknown test version " + hiveVersion);
        }
    }

    @Test
    public void testHiveBuiltInFunction() {
        FunctionDefinition fd = new HiveModule().getFunctionDefinition("reverse").get();
        HiveSimpleUDF udf = (HiveSimpleUDF) fd;

        DataType[] inputType = new DataType[] {DataTypes.STRING()};

        CallContext callContext = new HiveUDFCallContext(new Object[0], inputType);
        udf.getTypeInference(null).getOutputTypeStrategy().inferType(callContext);

        udf.open(null);

        assertEquals("cba", udf.eval("abc"));
    }

    @Test
    public void testNonExistFunction() {
        assertFalse(new HiveModule().getFunctionDefinition("nonexist").isPresent());
    }

    @Test
    public void testConstantArguments() {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

        tEnv.unloadModule("core");
        tEnv.loadModule("hive", new HiveModule());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('an', 'bn')").execute().collect());
        assertEquals("[anbn]", results.toString());

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('ab', cast('cdefghi' as varchar(5)))")
                                .execute()
                                .collect());
        assertEquals("[abcdefg]", results.toString());

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('ab',cast(12.34 as decimal(10,5)))")
                                .execute()
                                .collect());
        assertEquals("[ab12.34]", results.toString());

        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        "select concat(cast('2018-01-19' as date),cast('2019-12-27 17:58:23.385' as timestamp))")
                                .execute()
                                .collect());
        assertEquals("[2018-01-192019-12-27 17:58:23.385]", results.toString());

        // TODO: null cannot be a constant argument at the moment. This test will make more sense
        // when that changes.
        results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select concat('ab',cast(null as int))").execute().collect());
        assertEquals("[null]", results.toString());
    }

    @Test
    public void testDecimalReturnType() {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

        tEnv.unloadModule("core");
        tEnv.loadModule("hive", new HiveModule());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery("select negative(5.1)").execute().collect());

        assertEquals("[-5.1]", results.toString());
    }

    @Test
    public void testBlackList() {
        HiveModule hiveModule = new HiveModule();
        assertFalse(hiveModule.listFunctions().removeAll(HiveModule.BUILT_IN_FUNC_BLACKLIST));
        for (String banned : HiveModule.BUILT_IN_FUNC_BLACKLIST) {
            assertFalse(hiveModule.getFunctionDefinition(banned).isPresent());
        }
    }

    @Test
    public void testConstantReturnValue() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select str_to_map('a:1,b:2,c:3',',',':')")
                                .execute()
                                .collect());

        assertEquals("[{a=1, b=2, c=3}]", results.toString());
    }

    @Test
    public void testEmptyStringLiteralParameters() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());

        // UDF
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select regexp_replace('foobar','oo|ar','')")
                                .execute()
                                .collect());
        assertEquals("[fb]", results.toString());

        // GenericUDF
        results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select length('')").execute().collect());
        assertEquals("[0]", results.toString());
    }

    @Test
    public void testFunctionsNeedSessionState() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());
        tableEnv.loadModule("core", CoreModule.INSTANCE);

        tableEnv.sqlQuery("select current_timestamp,current_date").execute().collect();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select mod(-1,2),pmod(-1,2)").execute().collect());
        assertEquals("[-1,1]", results.toString());
    }

    @Test
    public void testCallUDFWithNoParam() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

        tableEnv.unloadModule("core");
        tableEnv.loadModule("hive", new HiveModule());
        tableEnv.loadModule("core", CoreModule.INSTANCE);

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select `array`(),`map`()").execute().collect());
        assertEquals("[[],{}]", results.toString());
    }
}
