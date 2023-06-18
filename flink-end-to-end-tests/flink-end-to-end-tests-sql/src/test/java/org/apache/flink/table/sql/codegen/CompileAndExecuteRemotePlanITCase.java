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

package org.apache.flink.table.sql.codegen;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-End tests for COMPILE AND EXECUTE PLAN statement with hdfs as remote uri. */
public class CompileAndExecuteRemotePlanITCase extends HdfsITCaseBase {

    private static final String TABLE1 = "message";
    private static final String TABLE2 = "employee";

    private String planDir;

    public CompileAndExecuteRemotePlanITCase(String executionMode) {
        super(executionMode);
    }

    @Override
    protected void createHDFS() {
        super.createHDFS();
        planDir = getRemotePlanDir();
    }

    @Override
    protected Map<String, String> generateReplaceVars() {
        Map<String, String> varsMap = super.generateReplaceVars();
        varsMap.put("$REMOTE_PLAN_DIR", planDir);
        varsMap.put("$TABLE1", TABLE1);
        varsMap.put("$TABLE2", TABLE2);
        return varsMap;
    }

    @Test
    public void testCompileAndExecutePlan() throws Exception {
        // COMPILE AND EXECUTE PLAN is not supported under batch mode
        Assume.assumeTrue(executionMode.equals("streaming"));
        Map<Path, List<String>> resultItems = new HashMap<>();
        resultItems.put(result.resolve(TABLE1), Arrays.asList("1,Meow", "2,Purr"));
        resultItems.put(result.resolve(TABLE2), Arrays.asList("1,Tom", "2,Jerry"));
        runAndCheckSQL("compile_and_execute_plan_e2e.sql", resultItems);

        assertPlanExists();
    }

    private String getRemotePlanDir() {
        return String.format(
                "hdfs://%s:%s/foo/bar",
                hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort());
    }

    private void assertPlanExists() throws IOException {
        org.apache.hadoop.fs.Path dirPath = new org.apache.hadoop.fs.Path(planDir);
        final RemoteIterator<LocatedFileStatus> iterable =
                dirPath.getFileSystem(hdConf).listFiles(dirPath, false);
        List<org.apache.hadoop.fs.Path> files = new ArrayList<>();
        while (iterable.hasNext()) {
            files.add(iterable.next().getPath());
        }
        assertThat(files)
                .containsExactlyInAnyOrder(
                        new org.apache.hadoop.fs.Path(dirPath, TABLE1 + ".json"),
                        new org.apache.hadoop.fs.Path(dirPath, TABLE2 + ".json"));
    }
}
