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
package org.apache.flink.api.java.table.test;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;


@RunWith(Parameterized.class)
public class FromCsvITCase extends MultipleProgramsTestBase {
    private String resultPath;
    private String expected;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    public FromCsvITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Before
    public void before() throws Exception {
        resultPath = tempFolder.newFile("result").toURI().toString();
    }

    @After
    public void after() throws Exception {
        compareResultsByLinesInMemory(expected, resultPath);
    }

    private String createInputData(String data) throws Exception {
        File file = tempFolder.newFile("input");
        Files.write(data, file, Charsets.UTF_8);

        return file.toURI().toString();
    }

    @Test
    public void testParameterizedTuple() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = new TableEnvironment();
        final String inputData = "ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10";
        final String dataPath = createInputData(inputData);
        Table table =
                tableEnv.fromCsvFile(dataPath, env, ParameterizedTuple3.class);

        DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
        ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();

        expected = "ABC,2.2,3\nDEF,5.1,5\nDEF,3.3,1\nGHI,3.3,10";
    }

    @Test
    public void testTuple() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = new TableEnvironment();
        final String inputData = "ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10";
        final String dataPath = createInputData(inputData);
        final Path path = new Path(Preconditions.checkNotNull(dataPath,
                "The file path may not be null."));
        Table table =
                tableEnv.fromCsvFile(path, env,
                        new Class[]{String.class, Float.class, Integer.class});

        DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
        ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();

        expected = "ABC,2.2,3\nDEF,5.1,5\nDEF,3.3,1\nGHI,3.3,10";
    }

    @Test
    public void testTupleWithOptions() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = new TableEnvironment();
        final String inputData = "a*b*c\nABC*2.20*3\nDEF*5.1*5\nDEF*3.30*1\n" +
                "#this is comment\nbadline\nGHI*3.30*10";
        final String dataPath = createInputData(inputData);
        final Path path = new Path(Preconditions.checkNotNull(dataPath,
                "The file path may not be null."));
        TableEnvironment.CsvOptions options = new TableEnvironment.CsvOptions();
        options.setCommentPrefix("#");
        options.setFieldDelimiter("*");
        options.setIgnoreInvalidLines(true);
        options.setSkipFirstLineAsHeader(true);
        options.setIncludedMask(new boolean[] {true, false, true});
        Table table =
                tableEnv.fromCsvFile(path, env,
                        new Class[]{String.class, Float.class, Integer.class},
                        "a,b,c", options);

        DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
        ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        expected = "ABC,3.0,0\nDEF,5.0,0\nDEF,1.0,0\nGHI,10.0,0";
    }
    @Test
    public void testPojo() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = new TableEnvironment();
        final String inputData = "ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10";
        final String dataPath = createInputData(inputData);
        final Path path = new Path(Preconditions.checkNotNull(dataPath,
                "The file path may not be null."));
        Table table =
                tableEnv.fromCsvFile(path, env, POJOItem.class, "f1,f2,f3");

        DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
        ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();

        expected = "ABC,2.2,3\nDEF,5.1,5\nDEF,3.3,1\nGHI,3.3,10";
    }

    public static class ParameterizedTuple3 extends Tuple3<String, Float, Integer>
            implements ParameterizedType
    {
        Type[] types = new Type[] {String.class, Float.class, Integer.class};
        @Override
        public Type getOwnerType() {
            return FromCsvITCase.class;
        }

        @Override
        public Type getRawType() {
            return Tuple3.class;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return types;
        }
    }

    public static class POJOItem
    {
        public String f1;
        public Double f2;
        public Integer f3;
        public POJOItem()
        {
            f1 = "";
            f2 = 0.0;
            f3 = 0;
        }
    }
}
