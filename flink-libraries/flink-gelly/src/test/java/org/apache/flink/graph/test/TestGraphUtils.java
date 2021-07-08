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

package org.apache.flink.graph.test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Utility methods and data for testing graph algorithms. */
public class TestGraphUtils {

    public static DataSet<Vertex<Long, Long>> getLongLongVertexData(ExecutionEnvironment env) {

        return env.fromCollection(getLongLongVertices());
    }

    public static DataSet<Edge<Long, Long>> getLongLongEdgeData(ExecutionEnvironment env) {

        return env.fromCollection(getLongLongEdges());
    }

    public static DataSet<Edge<Long, Long>> getLongLongEdgeInvalidSrcData(
            ExecutionEnvironment env) {
        List<Edge<Long, Long>> edges = getLongLongEdges();

        edges.remove(1);
        edges.add(new Edge<>(13L, 3L, 13L));

        return env.fromCollection(edges);
    }

    public static DataSet<Edge<Long, Long>> getLongLongEdgeInvalidTrgData(
            ExecutionEnvironment env) {
        List<Edge<Long, Long>> edges = getLongLongEdges();

        edges.remove(0);
        edges.add(new Edge<>(3L, 13L, 13L));

        return env.fromCollection(edges);
    }

    public static DataSet<Edge<Long, Long>> getLongLongEdgeInvalidSrcTrgData(
            ExecutionEnvironment env) {
        List<Edge<Long, Long>> edges = getLongLongEdges();
        edges.remove(0);
        edges.remove(1);
        edges.remove(2);
        edges.add(new Edge<>(13L, 3L, 13L));
        edges.add(new Edge<>(1L, 12L, 12L));
        edges.add(new Edge<>(13L, 33L, 13L));
        return env.fromCollection(edges);
    }

    public static DataSet<Edge<String, Long>> getStringLongEdgeData(ExecutionEnvironment env) {
        List<Edge<String, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>("1", "2", 12L));
        edges.add(new Edge<>("1", "3", 13L));
        edges.add(new Edge<>("2", "3", 23L));
        edges.add(new Edge<>("3", "4", 34L));
        edges.add(new Edge<>("3", "5", 35L));
        edges.add(new Edge<>("4", "5", 45L));
        edges.add(new Edge<>("5", "1", 51L));
        return env.fromCollection(edges);
    }

    public static DataSet<Tuple2<Long, Long>> getLongLongTuple2Data(ExecutionEnvironment env) {
        List<Tuple2<Long, Long>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>(1L, 10L));
        tuples.add(new Tuple2<>(2L, 20L));
        tuples.add(new Tuple2<>(3L, 30L));
        tuples.add(new Tuple2<>(4L, 40L));
        tuples.add(new Tuple2<>(6L, 60L));

        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple2<Long, Long>> getLongLongTuple2SourceData(
            ExecutionEnvironment env) {
        List<Tuple2<Long, Long>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>(1L, 10L));
        tuples.add(new Tuple2<>(1L, 20L));
        tuples.add(new Tuple2<>(2L, 30L));
        tuples.add(new Tuple2<>(3L, 40L));
        tuples.add(new Tuple2<>(3L, 50L));
        tuples.add(new Tuple2<>(4L, 60L));
        tuples.add(new Tuple2<>(6L, 70L));

        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple2<Long, Long>> getLongLongTuple2TargetData(
            ExecutionEnvironment env) {
        List<Tuple2<Long, Long>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>(2L, 10L));
        tuples.add(new Tuple2<>(3L, 20L));
        tuples.add(new Tuple2<>(3L, 30L));
        tuples.add(new Tuple2<>(4L, 40L));
        tuples.add(new Tuple2<>(6L, 50L));
        tuples.add(new Tuple2<>(6L, 60L));
        tuples.add(new Tuple2<>(1L, 70L));

        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple3<Long, Long, Long>> getLongLongLongTuple3Data(
            ExecutionEnvironment env) {
        List<Tuple3<Long, Long, Long>> tuples = new ArrayList<>();
        tuples.add(new Tuple3<>(1L, 2L, 12L));
        tuples.add(new Tuple3<>(1L, 3L, 13L));
        tuples.add(new Tuple3<>(2L, 3L, 23L));
        tuples.add(new Tuple3<>(3L, 4L, 34L));
        tuples.add(new Tuple3<>(3L, 6L, 36L));
        tuples.add(new Tuple3<>(4L, 6L, 46L));
        tuples.add(new Tuple3<>(6L, 1L, 61L));

        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple2<Long, DummyCustomParameterizedType<Float>>>
            getLongCustomTuple2Data(ExecutionEnvironment env) {
        List<Tuple2<Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>(1L, new DummyCustomParameterizedType<>(10, 10f)));
        tuples.add(new Tuple2<>(2L, new DummyCustomParameterizedType<>(20, 20f)));
        tuples.add(new Tuple2<>(3L, new DummyCustomParameterizedType<>(30, 30f)));
        tuples.add(new Tuple2<>(4L, new DummyCustomParameterizedType<>(40, 40f)));
        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple2<Long, DummyCustomParameterizedType<Float>>>
            getLongCustomTuple2SourceData(ExecutionEnvironment env) {
        List<Tuple2<Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>(1L, new DummyCustomParameterizedType<>(10, 10f)));
        tuples.add(new Tuple2<>(1L, new DummyCustomParameterizedType<>(20, 20f)));
        tuples.add(new Tuple2<>(2L, new DummyCustomParameterizedType<>(30, 30f)));
        tuples.add(new Tuple2<>(3L, new DummyCustomParameterizedType<>(40, 40f)));

        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple2<Long, DummyCustomParameterizedType<Float>>>
            getLongCustomTuple2TargetData(ExecutionEnvironment env) {
        List<Tuple2<Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>(2L, new DummyCustomParameterizedType<>(10, 10f)));
        tuples.add(new Tuple2<>(3L, new DummyCustomParameterizedType<>(20, 20f)));
        tuples.add(new Tuple2<>(3L, new DummyCustomParameterizedType<>(30, 30f)));
        tuples.add(new Tuple2<>(4L, new DummyCustomParameterizedType<>(40, 40f)));

        return env.fromCollection(tuples);
    }

    public static DataSet<Tuple3<Long, Long, DummyCustomParameterizedType<Float>>>
            getLongLongCustomTuple3Data(ExecutionEnvironment env) {
        List<Tuple3<Long, Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<>();
        tuples.add(new Tuple3<>(1L, 2L, new DummyCustomParameterizedType<>(10, 10f)));
        tuples.add(new Tuple3<>(1L, 3L, new DummyCustomParameterizedType<>(20, 20f)));
        tuples.add(new Tuple3<>(2L, 3L, new DummyCustomParameterizedType<>(30, 30f)));
        tuples.add(new Tuple3<>(3L, 4L, new DummyCustomParameterizedType<>(40, 40f)));

        return env.fromCollection(tuples);
    }

    /** A graph with invalid vertex ids. */
    public static DataSet<Vertex<Long, Long>> getLongLongInvalidVertexData(
            ExecutionEnvironment env) {
        List<Vertex<Long, Long>> vertices = getLongLongVertices();

        vertices.remove(0);
        vertices.add(new Vertex<>(15L, 1L));

        return env.fromCollection(vertices);
    }

    /** A graph that has at least one vertex with no ingoing/outgoing edges. */
    public static DataSet<Edge<Long, Long>> getLongLongEdgeDataWithZeroDegree(
            ExecutionEnvironment env) {
        List<Edge<Long, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 2L, 12L));
        edges.add(new Edge<>(1L, 4L, 14L));
        edges.add(new Edge<>(1L, 5L, 15L));
        edges.add(new Edge<>(2L, 3L, 23L));
        edges.add(new Edge<>(3L, 5L, 35L));
        edges.add(new Edge<>(4L, 5L, 45L));

        return env.fromCollection(edges);
    }

    /** Function that produces an ArrayList of vertices. */
    public static List<Vertex<Long, Long>> getLongLongVertices() {
        List<Vertex<Long, Long>> vertices = new ArrayList<>();
        vertices.add(new Vertex<>(1L, 1L));
        vertices.add(new Vertex<>(2L, 2L));
        vertices.add(new Vertex<>(3L, 3L));
        vertices.add(new Vertex<>(4L, 4L));
        vertices.add(new Vertex<>(5L, 5L));

        return vertices;
    }

    public static List<Vertex<Long, Boolean>> getLongBooleanVertices() {
        List<Vertex<Long, Boolean>> vertices = new ArrayList<>();
        vertices.add(new Vertex<>(1L, true));
        vertices.add(new Vertex<>(2L, true));
        vertices.add(new Vertex<>(3L, true));
        vertices.add(new Vertex<>(4L, true));
        vertices.add(new Vertex<>(5L, true));

        return vertices;
    }

    public static DataSet<Edge<Long, Long>> getDisconnectedLongLongEdgeData(
            ExecutionEnvironment env) {
        List<Edge<Long, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 2L, 12L));
        edges.add(new Edge<>(1L, 3L, 13L));
        edges.add(new Edge<>(2L, 3L, 23L));
        edges.add(new Edge<>(4L, 5L, 45L));

        return env.fromCollection(edges);
    }

    /** Function that produces an ArrayList of edges. */
    public static List<Edge<Long, Long>> getLongLongEdges() {
        List<Edge<Long, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 2L, 12L));
        edges.add(new Edge<>(1L, 3L, 13L));
        edges.add(new Edge<>(2L, 3L, 23L));
        edges.add(new Edge<>(3L, 4L, 34L));
        edges.add(new Edge<>(3L, 5L, 35L));
        edges.add(new Edge<>(4L, 5L, 45L));
        edges.add(new Edge<>(5L, 1L, 51L));

        return edges;
    }

    /** Test POJO. */
    public static class DummyCustomType implements Serializable {
        private static final long serialVersionUID = 1L;

        private int intField;
        private boolean booleanField;

        public DummyCustomType(int intF, boolean boolF) {
            this.intField = intF;
            this.booleanField = boolF;
        }

        public DummyCustomType() {
            this.intField = 0;
            this.booleanField = true;
        }

        public int getIntField() {
            return intField;
        }

        public void setIntField(int intF) {
            this.intField = intF;
        }

        public boolean getBooleanField() {
            return booleanField;
        }

        @Override
        public String toString() {
            return booleanField ? "(T," + intField + ")" : "(F," + intField + ")";
        }
    }

    /**
     * Generic test POJO.
     *
     * @param <T> field type
     */
    public static class DummyCustomParameterizedType<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private int intField;
        private T tField;

        public DummyCustomParameterizedType(int intF, T tF) {
            this.intField = intF;
            this.tField = tF;
        }

        public DummyCustomParameterizedType() {
            this.intField = 0;
            this.tField = null;
        }

        public int getIntField() {
            return intField;
        }

        public void setIntField(int intF) {
            this.intField = intF;
        }

        public void setTField(T tF) {
            this.tField = tF;
        }

        public T getTField() {
            return tField;
        }

        @Override
        public String toString() {
            return "(" + tField.toString() + "," + intField + ")";
        }
    }

    /** Method useful for suppressing sysout printing. */
    public static void pipeSystemOutToNull() {
        System.setOut(new PrintStream(new BlackholeOutputSteam()));
    }

    private static final class BlackholeOutputSteam extends java.io.OutputStream {
        @Override
        public void write(int b) {}
    }

    /**
     * utils for getting the second graph for the test of method difference().
     *
     * @param env - ExecutionEnvironment
     */
    public static DataSet<Edge<Long, Long>> getLongLongEdgeDataDifference(
            ExecutionEnvironment env) {
        return env.fromCollection(getLongLongEdgesForDifference());
    }

    public static DataSet<Edge<Long, Long>> getLongLongEdgeDataDifference2(
            ExecutionEnvironment env) {
        return env.fromCollection(getLongLongEdgesForDifference2());
    }

    public static DataSet<Vertex<Long, Long>> getLongLongVertexDataDifference(
            ExecutionEnvironment env) {
        return env.fromCollection(getVerticesForDifference());
    }

    public static List<Vertex<Long, Long>> getVerticesForDifference() {
        List<Vertex<Long, Long>> vertices = new ArrayList<>();
        vertices.add(new Vertex<>(1L, 1L));
        vertices.add(new Vertex<>(3L, 3L));
        vertices.add(new Vertex<>(6L, 6L));

        return vertices;
    }

    public static List<Edge<Long, Long>> getLongLongEdgesForDifference() {
        List<Edge<Long, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 3L, 13L));
        edges.add(new Edge<>(1L, 6L, 26L));
        edges.add(new Edge<>(6L, 3L, 63L));
        return edges;
    }

    public static List<Edge<Long, Long>> getLongLongEdgesForDifference2() {
        List<Edge<Long, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>(6L, 6L, 66L));
        return edges;
    }
}
