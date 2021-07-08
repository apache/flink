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

package org.apache.flink.graph.test.operations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.List;

/** Test graph creation from CSV. */
@RunWith(Parameterized.class)
public class GraphCreationWithCsvITCase extends MultipleProgramsTestBase {

    public GraphCreationWithCsvITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testCreateWithCsvFile() throws Exception {
        /*
         * Test with two Csv files one with Vertex Data and one with Edges data
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final String fileContent = "1,1\n" + "2,2\n" + "3,3\n";
        final FileInputSplit split = createTempFile(fileContent);
        final String fileContent2 = "1,2,ot\n" + "3,2,tt\n" + "3,1,to\n";
        final FileInputSplit split2 = createTempFile(fileContent2);

        Graph<Long, Long, String> graph =
                Graph.fromCsvReader(split.getPath().toString(), split2.getPath().toString(), env)
                        .types(Long.class, Long.class, String.class);

        List<Triplet<Long, Long, String>> result = graph.getTriplets().collect();

        expectedResult = "1,2,1,2,ot\n" + "3,2,3,2,tt\n" + "3,1,3,1,to\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testCsvWithNullEdge() throws Exception {
        /*
        Test fromCsvReader with edge and vertex path and nullvalue for edge
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final String vertexFileContent = "1,one\n" + "2,two\n" + "3,three\n";
        final String edgeFileContent = "1,2\n" + "3,2\n" + "3,1\n";
        final FileInputSplit split = createTempFile(vertexFileContent);
        final FileInputSplit edgeSplit = createTempFile(edgeFileContent);

        Graph<Long, String, NullValue> graph =
                Graph.fromCsvReader(split.getPath().toString(), edgeSplit.getPath().toString(), env)
                        .vertexTypes(Long.class, String.class);

        List<Triplet<Long, String, NullValue>> result = graph.getTriplets().collect();

        expectedResult =
                "1,2,one,two,(null)\n" + "3,2,three,two,(null)\n" + "3,1,three,one,(null)\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testCsvWithConstantValueMapper() throws Exception {
        /*
         *Test fromCsvReader with edge path and a mapper that assigns a Double constant as value
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final String fileContent = "1,2,ot\n" + "3,2,tt\n" + "3,1,to\n";
        final FileInputSplit split = createTempFile(fileContent);

        Graph<Long, Double, String> graph =
                Graph.fromCsvReader(split.getPath().toString(), new AssignDoubleValueMapper(), env)
                        .types(Long.class, Double.class, String.class);

        List<Triplet<Long, Double, String>> result = graph.getTriplets().collect();
        // graph.getTriplets().writeAsCsv(resultPath);
        expectedResult = "1,2,0.1,0.1,ot\n" + "3,1,0.1,0.1,to\n" + "3,2,0.1,0.1,tt\n";
        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testCreateWithOnlyEdgesCsvFile() throws Exception {
        /*
         * Test with one Csv file one with Edges data. Also tests the configuration method ignoreFistLineEdges()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final String fileContent2 = "header\n1,2,ot\n" + "3,2,tt\n" + "3,1,to\n";

        final FileInputSplit split2 = createTempFile(fileContent2);
        Graph<Long, NullValue, String> graph =
                Graph.fromCsvReader(split2.getPath().toString(), env)
                        .ignoreFirstLineEdges()
                        .ignoreCommentsVertices("hi")
                        .edgeTypes(Long.class, String.class);

        List<Triplet<Long, NullValue, String>> result = graph.getTriplets().collect();
        expectedResult =
                "1,2,(null),(null),ot\n" + "3,2,(null),(null),tt\n" + "3,1,(null),(null),to\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testCreateCsvFileDelimiterConfiguration() throws Exception {
        /*
         * Test with an Edge and Vertex csv file. Tests the configuration methods FieldDelimiterEdges and
         * FieldDelimiterVertices
         * Also tests the configuration methods LineDelimiterEdges and LineDelimiterVertices
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final String fileContent = "header\n1;1\n" + "2;2\n" + "3;3\n";

        final FileInputSplit split = createTempFile(fileContent);

        final String fileContent2 = "header|1:2:ot|" + "3:2:tt|" + "3:1:to|";

        final FileInputSplit split2 = createTempFile(fileContent2);

        Graph<Long, Long, String> graph =
                Graph.fromCsvReader(split.getPath().toString(), split2.getPath().toString(), env)
                        .ignoreFirstLineEdges()
                        .ignoreFirstLineVertices()
                        .fieldDelimiterEdges(":")
                        .fieldDelimiterVertices(";")
                        .lineDelimiterEdges("|")
                        .types(Long.class, Long.class, String.class);

        List<Triplet<Long, Long, String>> result = graph.getTriplets().collect();

        expectedResult = "1,2,1,2,ot\n" + "3,2,3,2,tt\n" + "3,1,3,1,to\n";

        compareResultAsTuples(result, expectedResult);
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    @SuppressWarnings("serial")
    private static final class AssignDoubleValueMapper implements MapFunction<Long, Double> {
        public Double map(Long value) {
            return 0.1d;
        }
    }

    private FileInputSplit createTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();

        OutputStreamWriter wrt =
                new OutputStreamWriter(new FileOutputStream(tempFile), Charset.forName("UTF-8"));
        wrt.write(content);
        wrt.close();

        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }
}
