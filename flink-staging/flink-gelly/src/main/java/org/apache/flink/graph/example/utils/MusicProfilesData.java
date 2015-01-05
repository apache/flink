package flink.graphs.example.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class MusicProfilesData {

	public static DataSet<Tuple3<String, String, Integer>> getUserSongTriplets(ExecutionEnvironment env) {
		List<Tuple3<String, String, Integer>> triplets = new ArrayList<Tuple3<String, String, Integer>>();
		
		triplets.add(new Tuple3<String, String, Integer>("user_1", "song_1", 100));
		triplets.add(new Tuple3<String, String, Integer>("user_1", "song_2", 10));
		triplets.add(new Tuple3<String, String, Integer>("user_1", "song_3", 20));
		triplets.add(new Tuple3<String, String, Integer>("user_1", "song_4", 30));
		triplets.add(new Tuple3<String, String, Integer>("user_1", "song_5", 1));
		
		triplets.add(new Tuple3<String, String, Integer>("user_2", "song_6", 40));
		triplets.add(new Tuple3<String, String, Integer>("user_2", "song_7", 10));
		triplets.add(new Tuple3<String, String, Integer>("user_2", "song_8", 3));
		
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_1", 100));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_2", 10));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_3", 20));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_8", 30));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_9", 1));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_10", 8));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_11", 90));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_12", 30));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_13", 34));
		triplets.add(new Tuple3<String, String, Integer>("user_3", "song_14", 17));
		
		triplets.add(new Tuple3<String, String, Integer>("user_4", "song_1", 100));
		triplets.add(new Tuple3<String, String, Integer>("user_4", "song_6", 10));
		triplets.add(new Tuple3<String, String, Integer>("user_4", "song_8", 20));
		triplets.add(new Tuple3<String, String, Integer>("user_4", "song_12", 30));
		triplets.add(new Tuple3<String, String, Integer>("user_4", "song_13", 1));
		triplets.add(new Tuple3<String, String, Integer>("user_4", "song_15", 1));
		
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_3", 300));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_4", 4));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_5", 5));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_8", 8));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_9", 9));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_10", 10));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_12", 12));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_13", 13));
		triplets.add(new Tuple3<String, String, Integer>("user_5", "song_15", 15));

		triplets.add(new Tuple3<String, String, Integer>("user_6", "song_6", 30));

		return env.fromCollection(triplets);
	}
	
	public static DataSet<String> getMismatches(ExecutionEnvironment env) {
		List<String> errors = new ArrayList<String>();
		errors.add("ERROR: <song_8 track_8> Sever");
		errors.add("ERROR: <song_15 track_15> Black Trees");
		return env.fromCollection(errors);
	}
}

