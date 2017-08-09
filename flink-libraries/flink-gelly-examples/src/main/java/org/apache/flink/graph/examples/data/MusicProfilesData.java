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

package org.apache.flink.graph.examples.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default data sets used for the Music Profiles example program.
 * If no parameters are given to the program, the default data sets are used.
 */
public class MusicProfilesData {

	public static DataSet<Tuple3<String, String, Integer>> getUserSongTriplets(ExecutionEnvironment env) {
		List<Tuple3<String, String, Integer>> triplets = new ArrayList<>();

		triplets.add(new Tuple3<>("user_1", "song_1", 100));
		triplets.add(new Tuple3<>("user_1", "song_2", 10));
		triplets.add(new Tuple3<>("user_1", "song_3", 20));
		triplets.add(new Tuple3<>("user_1", "song_4", 30));
		triplets.add(new Tuple3<>("user_1", "song_5", 1));

		triplets.add(new Tuple3<>("user_2", "song_6", 40));
		triplets.add(new Tuple3<>("user_2", "song_7", 10));
		triplets.add(new Tuple3<>("user_2", "song_8", 3));

		triplets.add(new Tuple3<>("user_3", "song_1", 100));
		triplets.add(new Tuple3<>("user_3", "song_2", 10));
		triplets.add(new Tuple3<>("user_3", "song_3", 20));
		triplets.add(new Tuple3<>("user_3", "song_8", 30));
		triplets.add(new Tuple3<>("user_3", "song_9", 1));
		triplets.add(new Tuple3<>("user_3", "song_10", 8));
		triplets.add(new Tuple3<>("user_3", "song_11", 90));
		triplets.add(new Tuple3<>("user_3", "song_12", 30));
		triplets.add(new Tuple3<>("user_3", "song_13", 34));
		triplets.add(new Tuple3<>("user_3", "song_14", 17));

		triplets.add(new Tuple3<>("user_4", "song_1", 100));
		triplets.add(new Tuple3<>("user_4", "song_6", 10));
		triplets.add(new Tuple3<>("user_4", "song_8", 20));
		triplets.add(new Tuple3<>("user_4", "song_12", 30));
		triplets.add(new Tuple3<>("user_4", "song_13", 1));
		triplets.add(new Tuple3<>("user_4", "song_15", 1));

		triplets.add(new Tuple3<>("user_5", "song_3", 300));
		triplets.add(new Tuple3<>("user_5", "song_4", 4));
		triplets.add(new Tuple3<>("user_5", "song_5", 5));
		triplets.add(new Tuple3<>("user_5", "song_8", 8));
		triplets.add(new Tuple3<>("user_5", "song_9", 9));
		triplets.add(new Tuple3<>("user_5", "song_10", 10));
		triplets.add(new Tuple3<>("user_5", "song_12", 12));
		triplets.add(new Tuple3<>("user_5", "song_13", 13));
		triplets.add(new Tuple3<>("user_5", "song_15", 15));

		triplets.add(new Tuple3<>("user_6", "song_6", 30));

		return env.fromCollection(triplets);
	}

	public static DataSet<String> getMismatches(ExecutionEnvironment env) {
		List<String> errors = new ArrayList<>();
		errors.add("ERROR: <song_8 track_8> Sever");
		errors.add("ERROR: <song_15 track_15> Black Trees");
		return env.fromCollection(errors);
	}

	public static final String USER_SONG_TRIPLETS = "user_1	song_1	100\n" + "user_1	song_5	200\n"
			+ "user_2	song_1	10\n" + "user_2	song_4	20\n"
			+ "user_3	song_2	3\n"
			+ "user_4	song_2	1\n" + "user_4	song_3	2\n"
			+ "user_5	song_3	30";

	public static final String MISMATCHES = "ERROR: <song_5 track_8> Angie";

	public static final String MAX_ITERATIONS = "2";

	public static final String TOP_SONGS_RESULT = "user_1	song_1\n" +
								"user_2	song_4\n" +
								"user_3	song_2\n" +
								"user_4	song_3\n" +
								"user_5	song_3";

	public static final String COMMUNITIES_RESULT = "user_1	1\n" +
								"user_2	1\n" +
								"user_3	3\n" +
								"user_4	3\n" +
								"user_5	4";
}
