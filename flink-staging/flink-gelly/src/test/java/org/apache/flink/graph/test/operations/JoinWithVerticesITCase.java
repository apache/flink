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

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JoinWithVerticesITCase extends MultipleProgramsTestBase {

	public JoinWithVerticesITCase(TestExecutionMode mode){
		super(mode);
	}

    private String expectedResult;

	@Test
	public void testJoinWithVertexSet() throws Exception {
		/*
		 * Test joinWithVertices with the input DataSet parameter identical
		 * to the vertex DataSet
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        Graph<Long, Long, Long> res = graph.joinWithVertices(graph.getVertices()
                        .map(new VertexToTuple2Map<Long, Long>()), new AddValuesMapper());

		DataSet<Vertex<Long,Long>> data = res.getVertices();
        List<Vertex<Long,Long>> result= data.collect();

       expectedResult = "1,2\n" +
	                "2,4\n" +
	                "3,6\n" +
	                "4,8\n" +
	                "5,10\n";
       
		compareResultAsTuples(result, expectedResult);
    }

	@Test
	public void testWithLessElements() throws Exception {
	/*
	 * Test joinWithVertices with the input DataSet passed as a parameter containing
	 * less elements than the vertex DataSet, but of the same type
	 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        Graph<Long, Long, Long> res = graph.joinWithVertices(graph.getVertices().first(3)
                        .map(new VertexToTuple2Map<Long, Long>()), new AddValuesMapper());

		DataSet<Vertex<Long,Long>> data = res.getVertices();
        List<Vertex<Long,Long>> result= data.collect();

        expectedResult = "1,2\n" +
	                "2,4\n" +
	                "3,6\n" +
	                "4,4\n" +
	                "5,5\n";
        
		compareResultAsTuples(result, expectedResult);
    }

	@Test
	public void testWithDifferentType() throws Exception {
	/*
	 * Test joinWithVertices with the input DataSet passed as a parameter containing
	 * less elements than the vertex DataSet and of a different type(Boolean)
	 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        Graph<Long, Long, Long> res = graph.joinWithVertices(graph.getVertices().first(3)
                        .map(new ProjectIdWithTrue()), new DoubleIfTrueMapper());

		DataSet<Vertex<Long,Long>> data = res.getVertices();
        List<Vertex<Long,Long>> result= data.collect();

        expectedResult = "1,2\n" +
	                "2,4\n" +
	                "3,6\n" +
	                "4,4\n" +
	                "5,5\n";
        
		compareResultAsTuples(result, expectedResult);
    }

	@Test
	public void testWithDifferentKeys() throws Exception {
		/*
		 * Test joinWithVertices with an input DataSet containing different keys than the vertex DataSet
		 * - the iterator becomes empty.
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        Graph<Long, Long, Long> res = graph.joinWithVertices(TestGraphUtils.getLongLongTuple2Data(env),
                new ProjectSecondMapper());

		DataSet<Vertex<Long,Long>> data = res.getVertices();
        List<Vertex<Long,Long>> result= data.collect();

        expectedResult = "1,10\n" +
	                "2,20\n" +
	                "3,30\n" +
	                "4,40\n" +
	                "5,5\n";
        
		compareResultAsTuples(result, expectedResult);
    }

	@Test
	public void testWithCustomType() throws Exception {
		/*
		 * Test joinWithVertices with a DataSet containing custom parametrised type input values
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        Graph<Long, Long, Long> res = graph.joinWithVertices(TestGraphUtils.getLongCustomTuple2Data(env),
                new CustomValueMapper());

		DataSet<Vertex<Long,Long>> data = res.getVertices();
        List<Vertex<Long,Long>> result= data.collect();

        expectedResult = "1,10\n" +
	                "2,20\n" +
	                "3,30\n" +
	                "4,40\n" +
	                "5,5\n";
        
		compareResultAsTuples(result, expectedResult);
    }

	@SuppressWarnings("serial")
	private static final class AddValuesMapper implements MapFunction<Tuple2<Long, Long>, Long> {
		public Long map(Tuple2<Long, Long> tuple) throws Exception {
			return tuple.f0 + tuple.f1;
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectIdWithTrue implements MapFunction<Vertex<Long, Long>, Tuple2<Long, Boolean>> {
        public Tuple2<Long, Boolean> map(Vertex<Long, Long> vertex) throws Exception {
            return new Tuple2<Long, Boolean>(vertex.getId(), true);
        }
    }

	@SuppressWarnings("serial")
	private static final class DoubleIfTrueMapper implements MapFunction<Tuple2<Long, Boolean>, Long> {
        public Long map(Tuple2<Long, Boolean> tuple) throws Exception {
            if(tuple.f1) {
                return tuple.f0 * 2;
            }
            else {
                return tuple.f0;
            }
        }
    }

	@SuppressWarnings("serial")
	private static final class ProjectSecondMapper implements MapFunction<Tuple2<Long, Long>, Long> {
        public Long map(Tuple2<Long, Long> tuple) throws Exception {
            return tuple.f1;
        }
    }

	@SuppressWarnings("serial")
	private static final class CustomValueMapper implements MapFunction<Tuple2<Long, DummyCustomParameterizedType<Float>>, Long> {
        public Long map(Tuple2<Long, DummyCustomParameterizedType<Float>> tuple) throws Exception {
            return (long) tuple.f1.getIntField();
        }
    }
}
