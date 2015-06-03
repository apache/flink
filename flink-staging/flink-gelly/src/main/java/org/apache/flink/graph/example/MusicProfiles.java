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

package org.apache.flink.graph.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.MusicProfilesData;
import org.apache.flink.graph.library.LabelPropagationAlgorithm;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class MusicProfiles implements ProgramDescription {

	/**
	 * This example demonstrates how to mix the "record" Flink API with the
	 * graph API. The input is a set <userId - songId - playCount> triplets and
	 * a set of bad records,i.e. song ids that should not be trusted. Initially,
	 * we use the record API to filter out the bad records. Then, we use the
	 * graph API to create a user -> song weighted bipartite graph and compute
	 * the top song (most listened) per user. Then, we use the record API again,
	 * to create a user-user similarity graph, based on common songs, where two
	 * users that listen to the same song are connected. Finally, we use the
	 * graph API to run the label propagation community detection algorithm on
	 * the similarity graph.
	 *
	 * The triplets input is expected to be given as one triplet per line,
	 * in the following format: "<userID>\t<songID>\t<playcount>".
	 *
	 * The mismatches input file is expected to contain one mismatch record per line,
	 * in the following format:
	 * "ERROR: <songID trackID> song_title"
	 *
	 * If no arguments are provided, the example runs with default data from {@link MusicProfilesData}.
	 */
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Read the user-song-play triplets.
		 */
		DataSet<Tuple3<String, String, Integer>> triplets = getUserSongTripletsData(env);

		/**
		 * Read the mismatches dataset and extract the songIDs
		 */
		DataSet<Tuple1<String>> mismatches = getMismatchesData(env).map(new ExtractMismatchSongIds());

		/**
		 * Filter out the mismatches from the triplets dataset
		 */
		DataSet<Tuple3<String, String, Integer>> validTriplets = triplets
				.coGroup(mismatches).where(1).equalTo(0)
				.with(new FilterOutMismatches());

		/**
		 * Create a user -> song weighted bipartite graph where the edge weights
		 * correspond to play counts
		 */
		Graph<String, NullValue, Integer> userSongGraph = Graph.fromTupleDataSet(validTriplets, env);

		/**
		 * Get the top track (most listened) for each user
		 */
		DataSet<Tuple2<String, String>> usersWithTopTrack = userSongGraph
				.groupReduceOnEdges(new GetTopSongPerUser(), EdgeDirection.OUT)
				.filter(new FilterSongNodes());

		if (fileOutput) {
			usersWithTopTrack.writeAsCsv(topTracksOutputPath, "\n", "\t");
		} else {
			usersWithTopTrack.print();
		}

		/**
		 * Create a user-user similarity graph, based on common songs, i.e. two
		 * users that listen to the same song are connected. For each song, we
		 * create an edge between each pair of its in-neighbors.
		 */
		DataSet<Edge<String, NullValue>> similarUsers = userSongGraph
				.getEdges().groupBy(1)
				.reduceGroup(new CreateSimilarUserEdges()).distinct();

		Graph<String, Long, NullValue> similarUsersGraph = Graph.fromDataSet(similarUsers,
				new MapFunction<String, Long>() {
					public Long map(String value) {
						return 1l;
					}
				}, env).getUndirected();

		/**
		 * Detect user communities using the label propagation library method
		 */

		// Initialize each vertex with a unique numeric label
		DataSet<Tuple2<String, Long>> idsWithInitialLabels = similarUsersGraph
				.getVertices().reduceGroup(new AssignInitialLabelReducer());

		// update the vertex values and run the label propagation algorithm
		DataSet<Vertex<String, Long>> verticesWithCommunity = similarUsersGraph
				.joinWithVertices(idsWithInitialLabels,
						new MapFunction<Tuple2<Long, Long>, Long>() {
							public Long map(Tuple2<Long, Long> value) {
								return value.f1;
							}
						}).run(new LabelPropagationAlgorithm<String>(maxIterations))
				.getVertices();

		if (fileOutput) {
			verticesWithCommunity.writeAsCsv(communitiesOutputPath, "\n", "\t");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute();
		} else {
			verticesWithCommunity.print();
		}

	}

	public static final class ExtractMismatchSongIds implements MapFunction<String, Tuple1<String>> {

		public Tuple1<String> map(String value) {
			String[] tokens = value.split("\\s+");
			String songId = tokens[1].substring(1);
			return new Tuple1<String>(songId);
		}
	}

	public static final class FilterOutMismatches implements CoGroupFunction<Tuple3<String, String, Integer>,
		Tuple1<String>, Tuple3<String, String, Integer>> {

		public void coGroup(Iterable<Tuple3<String, String, Integer>> triplets,
				Iterable<Tuple1<String>> invalidSongs, Collector<Tuple3<String, String, Integer>> out) {

			if (!invalidSongs.iterator().hasNext()) {
				// this is a valid triplet
				for (Tuple3<String, String, Integer> triplet : triplets) {
					out.collect(triplet);
				}
			}
		}
	}

	public static final class FilterSongNodes implements FilterFunction<Tuple2<String, String>> {
		public boolean filter(Tuple2<String, String> value) throws Exception {
			return !value.f1.equals("");
		}
	}

	public static final class GetTopSongPerUser	implements EdgesFunctionWithVertexValue<String, NullValue, Integer,
		Tuple2<String, String>> {

		public void iterateEdges(Vertex<String, NullValue> vertex,
				Iterable<Edge<String, Integer>> edges, Collector<Tuple2<String, String>> out) throws Exception {

			int maxPlaycount = 0;
			String topSong = "";
			for (Edge<String, Integer> edge : edges) {
				if (edge.getValue() > maxPlaycount) {
					maxPlaycount = edge.getValue();
					topSong = edge.getTarget();
				}
			}
			out.collect(new Tuple2<String, String>(vertex.getId(), topSong));
		}
	}

	public static final class CreateSimilarUserEdges implements GroupReduceFunction<Edge<String, Integer>,
		Edge<String, NullValue>> {

		public void reduce(Iterable<Edge<String, Integer>> edges, Collector<Edge<String, NullValue>> out) {
			List<String> listeners = new ArrayList<String>();
			for (Edge<String, Integer> edge : edges) {
				listeners.add(edge.getSource());
			}
			for (int i = 0; i < listeners.size() - 1; i++) {
				for (int j = i + 1; j < listeners.size(); j++) {
					out.collect(new Edge<String, NullValue>(listeners.get(i),
							listeners.get(j), NullValue.getInstance()));
				}
			}
		}
	}

	public static final class AssignInitialLabelReducer implements GroupReduceFunction<Vertex<String, Long>,
		Tuple2<String, Long>> {

		public void reduce(Iterable<Vertex<String, Long>> vertices,	Collector<Tuple2<String, Long>> out) {
			long label = 0;
			for (Vertex<String, Long> vertex : vertices) {
				out.collect(new Tuple2<String, Long>(vertex.getId(), label));
				label++;
			}
		}
	}

	@Override
	public String getDescription() {
		return "Music Profiles Example";
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String userSongTripletsInputPath = null;

	private static String mismatchesInputPath = null;

	private static String topTracksOutputPath = null;

	private static String communitiesOutputPath = null;

	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 5) {
				System.err.println("Usage: MusicProfiles <input user song triplets path>" +
						" <input song mismatches path> <output top tracks path> "
						+ "<output communities path> <num iterations>");
				return false;
			}

			fileOutput = true;
			userSongTripletsInputPath = args[0];
			mismatchesInputPath = args[1];
			topTracksOutputPath = args[2];
			communitiesOutputPath = args[3];
			maxIterations = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing Music Profiles example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: MusicProfiles <input user song triplets path>" +
					" <input song mismatches path> <output top tracks path> "
					+ "<output communities path> <num iterations>");
		}
		return true;
	}

	private static DataSet<Tuple3<String, String, Integer>> getUserSongTripletsData(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(userSongTripletsInputPath)
					.lineDelimiter("\n").fieldDelimiter("\t")
					.types(String.class, String.class, Integer.class);
		} else {
			return MusicProfilesData.getUserSongTriplets(env);
		}
	}

	private static DataSet<String> getMismatchesData(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readTextFile(mismatchesInputPath);
		} else {
			return MusicProfilesData.getMismatches(env);
		}
	}
}