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

package org.apache.flink.table.sql;

import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DefaultIndex;
import org.apache.flink.table.catalog.ImmutableColumnsConstraint;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.fail;

/** End-to-End tests for using remote jar. */
class UsingRemoteJarITCase extends HdfsITCaseBase {

    private static final ResolvedSchema USER_ORDER_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("user_name", DataTypes.STRING()),
                            Column.physical("order_cnt", DataTypes.BIGINT())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("user_name")),
                    Collections.singletonList(
                            DefaultIndex.newIndex("idx", Collections.singletonList("user_name"))),
                    ImmutableColumnsConstraint.immutableColumns(
                            "imt", Collections.singletonList("user_name")));

    private static final DebeziumJsonDeserializationSchema USER_ORDER_DESERIALIZATION_SCHEMA =
            createDebeziumDeserializationSchema(USER_ORDER_SCHEMA);

    private TestInfo testInfo;
    private org.apache.hadoop.fs.Path hdPath;
    private org.apache.hadoop.fs.FileSystem hdfs;

    @BeforeEach
    void captureTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @Override
    void createHDFS() {
        super.createHDFS();
        hdPath = new org.apache.hadoop.fs.Path("/test.jar");
        try {
            hdfs = hdPath.getFileSystem(hdConf);

            hdfs.copyFromLocalFile(
                    new org.apache.hadoop.fs.Path(SQL_TOOL_BOX_JAR.toString()), hdPath);
        } catch (IOException e) {
            fail("Failed to copy local test.jar to HDFS env" + e.getMessage(), e);
        }
    }

    @Override
    void destroyHDFS() {
        try {
            hdfs.delete(hdPath, false);
        } catch (IOException e) {
            fail("Failed to cleanup HDFS path" + e.getMessage(), e);
        }
        super.destroyHDFS();
    }

    @TestTemplate
    void testUdfInRemoteJar() throws Exception {
        runAndCheckSQL(
                "remote_jar_e2e.sql",
                Arrays.asList("+I[Bob, 2]", "+I[Alice, 1]"),
                raw ->
                        convertToMaterializedResult(
                                raw, USER_ORDER_SCHEMA, USER_ORDER_DESERIALIZATION_SCHEMA));
    }

    @TestTemplate
    void testCreateFunctionFromRemoteJarViaSqlClient() throws Exception {
        runAndCheckSQL(
                "sql_client_remote_jar_e2e.sql",
                Collections.singletonMap(result, Arrays.asList("+I[Bob, 2]", "+I[Alice, 1]")),
                Collections.singletonMap(
                        result,
                        raw ->
                                convertToMaterializedResult(
                                        raw, USER_ORDER_SCHEMA, USER_ORDER_DESERIALIZATION_SCHEMA)),
                Collections.singletonList(
                        URI.create(
                                String.format(
                                        "hdfs://%s:%s/%s",
                                        hdfsCluster.getURI().getHost(),
                                        hdfsCluster.getNameNodePort(),
                                        hdPath))));
    }

    @TestTemplate
    void testScalarUdfWhenCheckpointEnable() throws Exception {
        runAndCheckSQL(
                "scalar_udf_e2e.sql",
                Collections.singletonList(
                        "{\"before\":null,\"after\":{\"id\":1,\"str\":\"Hello Flink\"},\"op\":\"c\"}"));
    }

    @TestTemplate
    void testCreateTemporarySystemFunctionUsingRemoteJar() throws Exception {
        runAndCheckSQL(
                "create_function_using_remote_jar_e2e.sql",
                Arrays.asList("+I[Bob, 2]", "+I[Alice, 1]"),
                raw ->
                        convertToMaterializedResult(
                                raw, USER_ORDER_SCHEMA, USER_ORDER_DESERIALIZATION_SCHEMA));
    }

    @TestTemplate
    void testCreateCatalogFunctionUsingRemoteJar() throws Exception {
        runAndCheckSQL(
                "create_function_using_remote_jar_e2e.sql",
                Arrays.asList("+I[Bob, 2]", "+I[Alice, 1]"),
                raw ->
                        convertToMaterializedResult(
                                raw, USER_ORDER_SCHEMA, USER_ORDER_DESERIALIZATION_SCHEMA));
    }

    @TestTemplate
    void testCreateTemporaryCatalogFunctionUsingRemoteJar() throws Exception {
        Map<String, String> replaceVars = generateReplaceVars();
        replaceVars.put("$TEMPORARY", "TEMPORARY");
        runAndCheckSQL(
                "create_function_using_remote_jar_e2e.sql",
                Arrays.asList("+I[Bob, 2]", "+I[Alice, 1]"),
                raw ->
                        convertToMaterializedResult(
                                raw, USER_ORDER_SCHEMA, USER_ORDER_DESERIALIZATION_SCHEMA));
    }

    @Override
    Map<String, String> generateReplaceVars() {
        String remoteJarPath =
                String.format(
                        "hdfs://%s:%s/%s",
                        hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort(), hdPath);

        Map<String, String> varsMap = super.generateReplaceVars();
        varsMap.put("$JAR_PATH", remoteJarPath);
        String methodName = testInfo.getTestMethod().map(Method::getName).orElse("");
        if (methodName.startsWith("testCreateTemporarySystemFunction")) {
            varsMap.put("$TEMPORARY", "TEMPORARY SYSTEM");
        } else if (methodName.startsWith("testCreateTemporaryCatalogFunction")) {
            varsMap.put("$TEMPORARY", "TEMPORARY");
        } else if (methodName.startsWith("testCreateCatalogFunction")) {
            varsMap.put("$TEMPORARY", "");
        }
        return varsMap;
    }
}
