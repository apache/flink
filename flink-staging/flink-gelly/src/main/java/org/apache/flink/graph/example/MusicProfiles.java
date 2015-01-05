package flink.graphs.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.EdgeDirection;
import flink.graphs.EdgesFunctionWithVertexValue;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.utils.Tuple3ToEdgeMap;

public class MusicProfiles implements ProgramDescription {

	public static void main (String [] args) throws Exception {
    	
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    	/** 
    	 *  Read the user-song-play triplets
    	 *  The format is <userID>\t<songID>\t<playcount>
    	 */
    	DataSet<Tuple3<String, String, Integer>> triplets = env.readCsvFile(args[0])
    			.lineDelimiter("\n").fieldDelimiter('\t').types(String.class, String.class, Integer.class);
    	
    	/**
    	 *  Read the mismatches dataset and extract the songIDs
    	 *  The format is "ERROR: <songID trackID> song_title"
    	 */
    	DataSet<Tuple1<String>> mismatches = env.readTextFile(args[1]).map(new ExtractMismatchSongIds());

    	/**
    	 *  Filter out the mismatches from the triplets dataset
    	 */
    	DataSet<Tuple3<String, String, Integer>> validTriplets = triplets.coGroup(mismatches)
    			.where(1).equalTo(0).with(new FilterOutMismatches());

    	/**
    	 *  Create a user -> song weighted bipartite graph
    	 *  where the edge weights correspond to play counts
    	 */
    	DataSet<Edge<String, Integer>> userSongEdges = validTriplets.map(new Tuple3ToEdgeMap<String, Integer>());
    	Graph<String, NullValue, Integer> userSongGraph = Graph.create(userSongEdges, env);

    	/**
    	 *  Get the top track (most listened) for each user
    	 */
    	DataSet<Tuple2<String, String>> usersWithTopTrack = userSongGraph.reduceOnEdges(new GetTopSongPerUser(), 
    			EdgeDirection.OUT);

    	/**
    	 * Create a user-user similarity graph, based on common songs, 
    	 * i.e. two users that listen to the same song are connected.
    	 * For each song, we create an edge between each pair of its in-neighbors.
    	 */
    	DataSet<Edge<String, NullValue>> similarUsers = userSongGraph.getEdges().groupBy(1)
    			.reduceGroup(new CreateSimilarUserEdges()).distinct();
    	Graph<String, NullValue, NullValue> similarUsersGraph = Graph.create(similarUsers, env).getUndirected();

    }

    @SuppressWarnings("serial")
	public static final class ExtractMismatchSongIds implements MapFunction<String, Tuple1<String>> {
		public Tuple1<String> map(String value) {
			String[] tokens = value.split("\\s+"); 
			String songId = tokens[1].substring(1);
			return new Tuple1<String>(songId);
		}
    }

    @SuppressWarnings("serial")
	public static final class FilterOutMismatches implements CoGroupFunction<Tuple3<String, String, Integer>, 
    	Tuple1<String>, Tuple3<String, String, Integer>> {
		public void coGroup(
				Iterable<Tuple3<String, String, Integer>> triplets,
				Iterable<Tuple1<String>> invalidSongs,
				Collector<Tuple3<String, String, Integer>> out) {
			if (!invalidSongs.iterator().hasNext()) {
				// this is a valid triplet
				out.collect(triplets.iterator().next());
			}
		}
    }

    @SuppressWarnings("serial")
	public static final class GetTopSongPerUser implements EdgesFunctionWithVertexValue
		<String, NullValue, Integer, Tuple2<String, String>> {
		public Tuple2<String, String> iterateEdges(Vertex<String, NullValue> vertex,	
				Iterable<Edge<String, Integer>> edges) {
			int maxPlaycount = 0;
			String topSong = "";
			for (Edge<String, Integer> edge: edges) {
				if (edge.getValue() > maxPlaycount) {
					maxPlaycount = edge.getValue();
					topSong = edge.getTarget();
				}
			}
			return new Tuple2<String, String> (vertex.getId(), topSong);
		}
    }

    @SuppressWarnings("serial")
 	public static final class CreateSimilarUserEdges implements GroupReduceFunction<Edge<String, Integer>,
 		Edge<String, NullValue>> {
		public void reduce(Iterable<Edge<String, Integer>> edges, Collector<Edge<String, NullValue>> out) {
			List<String> listeners = new ArrayList<String>();
			for (Edge<String, Integer> edge : edges) {
				listeners.add(edge.getSource());
			}
			for (int i=0; i < listeners.size()-1; i++) {
				out.collect(new Edge<String, NullValue>(listeners.get(i), listeners.get(i+1)));
			}
		}
    }
	
	@Override
	public String getDescription() {
		return null;
	}

}
