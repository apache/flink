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

package org.apache.flink.test.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;

import org.junit.Assert;

import java.util.List;

/** Tests for the DataSource. */
public class DataSourceITCase extends JavaProgramTestBase {

    private String inputPath;

    @Override
    protected void preSubmit() throws Exception {
        inputPath = createTempFile("input", "ab\n" + "cd\n" + "ef\n");
    }

    @Override
    protected void testProgram() throws Exception {
        /*
         * Test passing a configuration object to an input format
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration ifConf = new Configuration();
        ifConf.setString("prepend", "test");

        DataSet<String> ds =
                env.createInput(new TestInputFormat(new Path(inputPath))).withParameters(ifConf);
        List<String> result = ds.collect();

        String expectedResult = "ab\n" + "cd\n" + "ef\n";

        compareResultAsText(result, expectedResult);
    }

    private static class TestInputFormat extends TextInputFormat {
        private static final long serialVersionUID = 1L;

        public TestInputFormat(Path filePath) {
            super(filePath);
        }

        @Override
        public void configure(Configuration parameters) {
            super.configure(parameters);

            Assert.assertNotNull(parameters.getString("prepend", null));
            Assert.assertEquals("test", parameters.getString("prepend", null));
        }
    }
}
