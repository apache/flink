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

package org.apache.flink.api.java.sca;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.sca.UdfAnalyzerTest.compareAnalyzerResultWithAnnotationsDualInput;
import static org.apache.flink.api.java.sca.UdfAnalyzerTest.compareAnalyzerResultWithAnnotationsDualInputWithKeys;
import static org.apache.flink.api.java.sca.UdfAnalyzerTest.compareAnalyzerResultWithAnnotationsSingleInput;
import static org.apache.flink.api.java.sca.UdfAnalyzerTest.compareAnalyzerResultWithAnnotationsSingleInputWithKeys;

/**
 * This class contains some more advanced tests based on examples from "real world".
 * The examples are not complete copies. They are modified at some points to reduce
 * dependencies to other classes.
 */
@SuppressWarnings("serial")
public class UdfAnalyzerExamplesTest {

	// --------------------------------------------------------------------------------------------
	// EnumTriangles
	// --------------------------------------------------------------------------------------------

	private static class Edge extends Tuple2<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		public static final int V1 = 0;
		public static final int V2 = 1;

		public Edge() {}

		public Edge(final Integer v1, final Integer v2) {
			this.setFirstVertex(v1);
			this.setSecondVertex(v2);
		}

		public Integer getFirstVertex() {
			return this.getField(V1);
		}

		public Integer getSecondVertex() {
			return this.getField(V2);
		}

		public void setFirstVertex(final Integer vertex1) {
			this.setField(vertex1, V1);
		}

		public void setSecondVertex(final Integer vertex2) {
			this.setField(vertex2, V2);
		}

		public void copyVerticesFromTuple2(Tuple2<Integer, Integer> t) {
			this.setFirstVertex(t.f0);
			this.setSecondVertex(t.f1);
		}

		public void flipVertices() {
			Integer tmp = this.getFirstVertex();
			this.setFirstVertex(this.getSecondVertex());
			this.setSecondVertex(tmp);
		}
	}

	private static class Triad extends Tuple3<Integer, Integer, Integer> {
		private static final long serialVersionUID = 1L;

		public static final int V1 = 0;
		public static final int V2 = 1;
		public static final int V3 = 2;

		public Triad() {
		}

		public void setFirstVertex(final Integer vertex1) {
			this.setField(vertex1, V1);
		}

		public void setSecondVertex(final Integer vertex2) {
			this.setField(vertex2, V2);
		}

		public void setThirdVertex(final Integer vertex3) {
			this.setField(vertex3, V3);
		}
	}

	@ForwardedFields("0")
	private static class TriadBuilder implements GroupReduceFunction<Edge, Triad> {
		private final List<Integer> vertices = new ArrayList<Integer>();
		private final Triad outTriad = new Triad();

		@Override
		public void reduce(Iterable<Edge> edgesIter, Collector<Triad> out) throws Exception {

			final Iterator<Edge> edges = edgesIter.iterator();

			// clear vertex list
			vertices.clear();

			// read first edge
			Edge firstEdge = edges.next();
			outTriad.setFirstVertex(firstEdge.getFirstVertex());

			vertices.add(firstEdge.getSecondVertex());

			// build and emit triads
			while (edges.hasNext()) {
				Integer higherVertexId = edges.next().getSecondVertex();

				// combine vertex with all previously read vertices
				for (Integer lowerVertexId : vertices) {
					outTriad.setSecondVertex(lowerVertexId);
					outTriad.setThirdVertex(higherVertexId);
					out.collect(outTriad);
				}
				vertices.add(higherVertexId);
			}
		}
	}

	@Test
	public void testEnumTrianglesBasicExamplesTriadBuilder() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, TriadBuilder.class,
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}),
				TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Integer>>(){}),
				new String[] { "0" });
	}

	@ForwardedFields("0;1")
	private static class TupleEdgeConverter implements MapFunction<Tuple2<Integer, Integer>, Edge> {
		private final Edge outEdge = new Edge();

		@Override
		public Edge map(Tuple2<Integer, Integer> t) throws Exception {
			outEdge.copyVerticesFromTuple2(t);
			return outEdge;
		}
	}

	@Test
	public void testEnumTrianglesBasicExamplesTupleEdgeConverter() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, TupleEdgeConverter.class,
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	private static class EdgeDuplicator implements FlatMapFunction<Edge, Edge> {
		@Override
		public void flatMap(Edge edge, Collector<Edge> out) throws Exception {
			out.collect(edge);
			edge.flipVertices();
			out.collect(edge);
		}
	}

	@Test
	public void testEnumTrianglesOptExamplesEdgeDuplicator() {
		compareAnalyzerResultWithAnnotationsSingleInput(FlatMapFunction.class, EdgeDuplicator.class,
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	private static class DegreeCounter implements GroupReduceFunction<Edge, Edge> {
		final ArrayList<Integer> otherVertices = new ArrayList<Integer>();
		final Edge outputEdge = new Edge();

		@Override
		public void reduce(Iterable<Edge> edgesIter, Collector<Edge> out) {

			Iterator<Edge> edges = edgesIter.iterator();
			otherVertices.clear();

			// get first edge
			Edge edge = edges.next();
			Integer groupVertex = edge.getFirstVertex();
			this.otherVertices.add(edge.getSecondVertex());

			// get all other edges (assumes edges are sorted by second vertex)
			while (edges.hasNext()) {
				edge = edges.next();
				Integer otherVertex = edge.getSecondVertex();
				// collect unique vertices
				if (!otherVertices.contains(otherVertex) && !otherVertex.equals(groupVertex)) {
					this.otherVertices.add(otherVertex);
				}
			}

			// emit edges
			for (Integer otherVertex : this.otherVertices) {
				if (groupVertex < otherVertex) {
					outputEdge.setFirstVertex(groupVertex);
					outputEdge.setSecondVertex(otherVertex);
				} else {
					outputEdge.setFirstVertex(otherVertex);
					outputEdge.setSecondVertex(groupVertex);
				}
				out.collect(outputEdge);
			}
		}
	}

	@Test
	public void testEnumTrianglesOptExamplesDegreeCounter() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, DegreeCounter.class,
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}),
				new String[] { "0" });
	}

	// --------------------------------------------------------------------------------------------
	// KMeans
	// --------------------------------------------------------------------------------------------

	/**
	 * Representation of point int 2d plane.
	 */
	public static class Point implements Serializable {
		public double x, y;

		public Point() {}

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public Point add(Point other) {
			x += other.x;
			y += other.y;
			return this;
		}

		public Point div(long val) {
			x /= val;
			y /= val;
			return this;
		}

		public void clear() {
			x = y = 0.0;
		}

		@Override
		public String toString() {
			return x + " " + y;
		}
	}

	/**
	 * Representation of centroid in 2d plane.
	 */
	public static class Centroid extends Point {
		public int id;

		public Centroid() {}

		public Centroid(int id, double x, double y) {
			super(x, y);
			this.id = id;
		}

		public Centroid(int id, Point p) {
			super(p.x, p.y);
			this.id = id;
		}

		@Override
		public String toString() {
			return id + " " + super.toString();
		}
	}

	@ForwardedFields("0")
	private static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {
		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}

	@Test
	public void testKMeansExamplesCentroidAccumulator() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(ReduceFunction.class, CentroidAccumulator.class,
				TypeInformation.of(new TypeHint<Tuple3<Integer, Point, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple3<Integer, Point, Long>>(){}),
				new String[] { "0" });
	}

	@ForwardedFields("0->id")
	private static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {
		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}

	@Test
	public void testKMeansExamplesCentroidAverager() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, CentroidAverager.class,
				TypeInformation.of(new TypeHint<Tuple3<Integer, Point, Long>>(){}),
				TypeInformation.of(new TypeHint<Centroid>(){}));
	}

	// --------------------------------------------------------------------------------------------
	// ConnectedComponents
	// --------------------------------------------------------------------------------------------

	private static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
	}

	@Test
	public void testConnectedComponentsExamplesUndirectEdge() {
		compareAnalyzerResultWithAnnotationsSingleInput(FlatMapFunction.class, UndirectEdge.class,
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	@ForwardedFieldsFirst("*")
	private static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	@Test
	public void testConnectedComponentsExamplesComponentIdFilter() {
		compareAnalyzerResultWithAnnotationsDualInput(FlatJoinFunction.class, ComponentIdFilter.class,
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	@ForwardedFields("*->f0;*->f1")
	private static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {
		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<T, T>(vertex, vertex);
		}
	}

	@Test
	public void testConnectedComponentsExamplesDuplicateValue() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, DuplicateValue.class,
			Types.LONG, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	@ForwardedFieldsFirst("f1->f1")
	@ForwardedFieldsSecond("f1->f0")
	private static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}

	@Test
	public void testConnectedComponentsExamplesNeighborWithComponentIDJoin() {
		compareAnalyzerResultWithAnnotationsDualInput(JoinFunction.class, NeighborWithComponentIDJoin.class,
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	// --------------------------------------------------------------------------------------------
	// WebLogAnalysis
	// --------------------------------------------------------------------------------------------

	@ForwardedFieldsFirst("f1")
	private static class AntiJoinVisits implements CoGroupFunction<Tuple3<Integer, String, Integer>, Tuple1<String>, Tuple3<Integer, String, Integer>> {
		@Override
		public void coGroup(Iterable<Tuple3<Integer, String, Integer>> ranks, Iterable<Tuple1<String>> visits, Collector<Tuple3<Integer, String, Integer>> out) {
			// Check if there is a entry in the visits relation
			if (!visits.iterator().hasNext()) {
				for (Tuple3<Integer, String, Integer> next : ranks) {
					// Emit all rank pairs
					out.collect(next);
				}
			}
		}
	}

	@Test
	public void testWebLogAnalysisExamplesAntiJoinVisits() {
		compareAnalyzerResultWithAnnotationsDualInputWithKeys(CoGroupFunction.class, AntiJoinVisits.class,
				TypeInformation.of(new TypeHint<Tuple3<Integer, String, Integer>>(){}),
				TypeInformation.of(new TypeHint<Tuple1<String>>(){}),
				TypeInformation.of(new TypeHint<Tuple3<Integer, String, Integer>>(){}),
				new String[] { "1" }, new String[] { "0" });
	}

	// --------------------------------------------------------------------------------------------
	// PageRankBasic
	// --------------------------------------------------------------------------------------------

	@ForwardedFields("0")
	private static class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {
		private final ArrayList<Long> neighbors = new ArrayList<Long>();

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
			neighbors.clear();
			Long id = 0L;

			for (Tuple2<Long, Long> n : values) {
				id = n.f0;
				neighbors.add(n.f1);
			}
			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}
	}

	@Test
	public void testPageRankBasicExamplesBuildOutgoingEdgeList() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, BuildOutgoingEdgeList.class,
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
				TypeInformation.of(new TypeHint<Tuple2<Long, Long[]>>(){}),
				new String[] { "0" });
	}

	// --------------------------------------------------------------------------------------------
	// LogisticRegression
	// --------------------------------------------------------------------------------------------

	private static class Vector extends Tuple1<double[]> {
		public Vector() {
			// default constructor needed for instantiation during serialization
		}

		public Vector(int size) {
			double[] components = new double[size];
			for (int i = 0; i < size; i++) {
				components[i] = 0.0;
			}
			setComponents(components);
		}

		public double[] getComponents() {
			return this.f0;
		}

		public double getComponent(int i) {
			return this.f0[i];
		}

		public void setComponent(int i, double value) {
			this.f0[i] = value;
		}

		public void setComponents(double[] components) {
			this.f0 = components;
		}
	}

	private static class Gradient extends Vector {
		public Gradient() {
			// default constructor needed for instantiation during serialization
		}

		public Gradient(int size) {
			super(size);
		}
	}

	private static class PointWithLabel extends Tuple2<Integer, double[]> {
		public double[] getFeatures() {
			return this.f1;
		}

		public double getFeature(int i) {
			return this.f1[i];
		}

		public void setFeatures(double[] features) {
			this.f1 = features;
		}

		public Integer getLabel() {
			return this.f0;
		}

		public void setLabel(Integer label) {
			this.f0 = label;
		}
	}

	private static class SumGradient implements ReduceFunction<Gradient> {
		@Override
		public Gradient reduce(Gradient gradient1, Gradient gradient2) throws Exception {
			// grad(i) +=
			for (int i = 0; i < gradient1.getComponents().length; i++) {
				gradient1.setComponent(i, gradient1.getComponent(i) + gradient2.getComponent(i));
			}

			return gradient1;
		}
	}

	@Test
	public void testLogisticRegressionExamplesSumGradient() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(ReduceFunction.class, SumGradient.class,
				TypeInformation.of(new TypeHint<Tuple1<Double>>(){}),
				TypeInformation.of(new TypeHint<Tuple1<Double>>(){}),
				new String[] { "0" });
	}

	private static class PointParser implements MapFunction<String, PointWithLabel> {
		@Override
		public PointWithLabel map(String value) throws Exception {
			PointWithLabel p = new PointWithLabel();

			String[] split = value.split(",");
			double[] features = new double[42];

			int a = 0;
			for (int i = 0; i < split.length; i++) {

				if (i == 42 - 1) {
					p.setLabel(Integer.valueOf(split[i].trim().substring(0, 1)));
				} else {
					if (a < 42 && !split[i].trim().isEmpty()) {
						features[a++] = Double.parseDouble(split[i].trim());
					}
				}
			}

			p.setFeatures(features);
			return p;
		}
	}

	@Test
	public void testLogisticRegressionExamplesPointParser() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, PointParser.class,
				Types.STRING,
				TypeInformation.of(new TypeHint<Tuple2<Integer, double[]>>(){}));
	}

	// --------------------------------------------------------------------------------------------
	// Canopy
	// --------------------------------------------------------------------------------------------

	private static class Document extends Tuple5<Integer, Boolean, Boolean, String, String> {
		public Document() {
			// default constructor needed for instantiation during serialization
		}

		public Document(Integer docId, Boolean isCenter, Boolean isInSomeT2, String canopyCenters, String words) {
			super(docId, isCenter, isInSomeT2, canopyCenters, words);
		}

		public Document(Integer docId) {
			super(docId, null, null, null, null);
		}
	}

	private static class MessageBOW implements FlatMapFunction<String, Tuple2<Integer, String>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
			String[] splits = value.split(" ");
			if (splits.length < 2) {
				return;
			}
			out.collect(new Tuple2<Integer, String>(Integer.valueOf(splits[0]), splits[1]));
		}
	}

	@Test
	public void testCanopyExamplesMassageBOW() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, PointParser.class,
				Types.STRING,
				TypeInformation.of(new TypeHint<Tuple2<Integer, String>>(){}));
	}

	@ForwardedFields("0")
	private static class DocumentReducer implements GroupReduceFunction<Tuple2<Integer, String>, Document> {
		@Override
		public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<Document> out) throws Exception {
			Iterator<Tuple2<Integer, String>> it = values.iterator();
			Tuple2<Integer, String> first = it.next();
			Integer docId = first.f0;
			StringBuilder builder = new StringBuilder(first.f1);
			while (it.hasNext()) {
				builder.append("-").append(it.next().f1);
			}
			out.collect(new Document(docId, false, false, "", builder.toString()));
		}
	}

	@Test
	public void testCanopyExamplesDocumentReducer() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, DocumentReducer.class,
				TypeInformation.of(new TypeHint<Tuple2<Integer, String>>(){}),
				TypeInformation.of(new TypeHint<Tuple5<Integer, Boolean, Boolean, String, String>>(){}),
				new String[] { "0" });
	}

	@ForwardedFields("0;4")
	private static class MapToCenter implements MapFunction<Document, Document> {
		private Document center;

		@Override
		public Document map(Document value) throws Exception {
			if (center != null) {
				final float similarity = 42f;
				final boolean isEqual = value.f0.equals(center.f0);
				value.f1 = isEqual;
				value.f2 = isEqual || similarity > 42;
				if (!value.f3.contains(center.f0.toString() + ";") && (similarity > 42 || isEqual)) {
					value.f3 += center.f0.toString() + ";";
				}
			}
			return value;
		}
	}

	@Test
	public void testCanopyExamplesMapToCenter() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, MapToCenter.class,
				TypeInformation.of(new TypeHint<Tuple5<Integer, Boolean, Boolean, String, String>>(){}),
				TypeInformation.of(new TypeHint<Tuple5<Integer, Boolean, Boolean, String, String>>(){}));
	}

	// --------------------------------------------------------------------------------------------
	// K-Meanspp
	// --------------------------------------------------------------------------------------------

	/**
	 * Representation of document with word frequencies.
	 */
	public static class DocumentWithFreq implements Serializable {
		private static final long serialVersionUID = -8646398807053061675L;

		public Map<String, Double> wordFreq = new HashMap<String, Double>();

		public Integer id;

		public DocumentWithFreq() {
			id = -1;
		}

		public DocumentWithFreq(Integer id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return Integer.toString(id);
		}
	}

	@ForwardedFields("0->id")
	private static final class RecordToDocConverter implements GroupReduceFunction<Tuple3<Integer, Integer, Double>, DocumentWithFreq> {
		private static final long serialVersionUID = -8476366121490468956L;

		@Override
		public void reduce(Iterable<Tuple3<Integer, Integer, Double>> values, Collector<DocumentWithFreq> out) throws Exception {
			Iterator<Tuple3<Integer, Integer, Double>> it = values.iterator();
			if (it.hasNext()) {
				Tuple3<Integer, Integer, Double> elem = it.next();
				DocumentWithFreq doc = new DocumentWithFreq(elem.f0);
				doc.wordFreq.put(elem.f1.toString(), elem.f2);

				while (it.hasNext()) {
					elem = it.next();
					doc.wordFreq.put(elem.f1.toString(), elem.f2);
				}
				out.collect(doc);
			}
		}
	}

	@Test
	public void testKMeansppExamplesRecordToDocConverter() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, RecordToDocConverter.class,
				TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Double>>(){}),
				TypeInformation.of(new TypeHint<DocumentWithFreq>(){}),
				new String[] { "0" });
	}
}
