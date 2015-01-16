package flink.graphs.example;

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
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.EdgeDirection;
import flink.graphs.EdgesFunctionWithVertexValue;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.example.utils.MusicProfilesData;
import flink.graphs.library.LabelPropagation;
import flink.graphs.utils.Tuple3ToEdgeMap;

@SuppressWarnings("serial")
public class MusicProfiles implements ProgramDescription {

	/**
	 * This example demonstrates how to mix the "record" Flink API with the graph API.
	 * The input is a set <userId - songId - playCount> triplets and a set of
	 * bad records,i.e. song ids that should not be trusted.
	 * Initially, we use the record API to filter out the bad records.
	 * Then, we use the graph API to create a user -> song weighted bipartite graph
	 * and compute the top song (most listened) per user.
	 * Then, we use the record API again, to create a user-user similarity graph, 
	 * based on common songs, where two users that listen to the same song are connected.
	 * Finally, we use the graph API to run the label propagation community detection algorithm
	 * on the similarity graph.
	 */
	public static void main (String [] args) throws Exception {
    	
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    	final int numIterations = 10;

    	/** 
    	 *  Read the user-song-play triplets
    	 *  The format is <userID>\t<songID>\t<playcount>
    	 */
    	DataSet<Tuple3<String, String, Integer>> triplets = MusicProfilesData.getUserSongTriplets(env);

    	/**
    	 *  Read the mismatches dataset and extract the songIDs
    	 *  The format is "ERROR: <songID trackID> song_title"
    	 */
    	DataSet<Tuple1<String>> mismatches = MusicProfilesData.getMismatches(env).map(new ExtractMismatchSongIds());

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
    			EdgeDirection.OUT).filter(new FilterSongNodes());

    	usersWithTopTrack.print();

    	/**
    	 * Create a user-user similarity graph, based on common songs, 
    	 * i.e. two users that listen to the same song are connected.
    	 * For each song, we create an edge between each pair of its in-neighbors.
    	 */
    	DataSet<Edge<String, NullValue>> similarUsers = userSongGraph.getEdges().groupBy(1)
    			.reduceGroup(new CreateSimilarUserEdges()).distinct();

    	Graph<String, Long, NullValue> similarUsersGraph = Graph.create(similarUsers,

    			new MapFunction<String, Long>() {
					public Long map(String value) { return 1l; }

    	}, env).getUndirected();

    	/**
    	 * Detect user communities using the label propagation library method
    	 */

    	// Initialize each vertex with a unique numeric label
    	DataSet<Tuple2<String, Long>> idsWithInitialLabels = similarUsersGraph.getVertices()
    			.reduceGroup(new AssignInitialLabelReducer());

    	// update the vertex values and run the label propagation algorithm
    	DataSet<Vertex<String, Long>> verticesWithCommunity = similarUsersGraph.joinWithVertices(idsWithInitialLabels,
    					new MapFunction<Tuple2<Long, Long>, Long>() {
							public Long map(Tuple2<Long, Long> value) {	return value.f1; }
						})
    			.run(new LabelPropagation<String>(numIterations)).getVertices();

    	verticesWithCommunity.print();

    	env.execute();
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
		public void coGroup(
				Iterable<Tuple3<String, String, Integer>> triplets,
				Iterable<Tuple1<String>> invalidSongs,
				Collector<Tuple3<String, String, Integer>> out) {
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

 	public static final class CreateSimilarUserEdges implements GroupReduceFunction<Edge<String, Integer>,
 		Edge<String, NullValue>> {
		public void reduce(Iterable<Edge<String, Integer>> edges, Collector<Edge<String, NullValue>> out) {
			List<String> listeners = new ArrayList<String>();
			for (Edge<String, Integer> edge : edges) {
				listeners.add(edge.getSource());
			}
			for (int i=0; i < listeners.size()-1; i++) {
				out.collect(new Edge<String, NullValue>(listeners.get(i), listeners.get(i+1),
						NullValue.getInstance()));
			}
		}
    }

 	public static final class AssignInitialLabelReducer implements GroupReduceFunction<Vertex<String, Long>,
 		Tuple2<String, Long>> {
 		public void reduce(Iterable<Vertex<String, Long>> vertices, Collector<Tuple2<String, Long>> out) {
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
}
