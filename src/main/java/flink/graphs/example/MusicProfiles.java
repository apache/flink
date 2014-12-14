package flink.graphs.example;

import java.util.Iterator;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CoGroupFunction;
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
import flink.graphs.EdgesFunction;
import flink.graphs.Graph;
import flink.graphs.utils.Tuple3ToEdgeMap;

public class MusicProfiles implements ProgramDescription {

    @SuppressWarnings("serial")
	public static void main (String [] args) throws Exception {
    	
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    	/** read the user-song-play triplets
    	 *  The format is <userID>\t<songID>\t<playcount>
    	 */
    	DataSet<Tuple3<String, String, Integer>> triplets = env.readCsvFile(args[0])
    			.lineDelimiter("\n").fieldDelimiter('\t').types(String.class, String.class, Integer.class);
    	
    	/**
    	 *  read the mismatches dataset and extract the songIDs
    	 *  The format is "ERROR: <songID trackID> song_title"
    	 */
    	DataSet<Tuple1<String>> mismatches = env.readTextFile(args[1]).map(
    			new MapFunction<String, Tuple1<String>>() {
					public Tuple1<String> map(String value) {
						// TODO Auto-generated method stub
						return null;
					}
		});

    	// filter out the mismatches from the triplets dataset
    	DataSet<Tuple3<String, String, Integer>> validTriplets = triplets.coGroup(mismatches)
    			.where(1).equalTo(0).with(new CoGroupFunction<Tuple3<String, String, Integer>, Tuple1<String>, 
    					Tuple3<String, String, Integer>>() {
							public void coGroup(
									Iterable<Tuple3<String, String, Integer>> triplets,
									Iterable<Tuple1<String>> invalidSongs,
									Collector<Tuple3<String, String, Integer>> out) {
								if (!invalidSongs.iterator().hasNext()) {
									// this is a valid triplet
									out.collect(triplets.iterator().next());
								}
							}
				});

    	// Create a user -> song weighted bipartite graph
    	// where the edge weights correspond to play counts
    	DataSet<Edge<String, Integer>> userSongEdges = validTriplets.map(
    			new Tuple3ToEdgeMap<String, Integer>());

    	Graph<String, NullValue, Integer> userSongGraph = Graph.create(userSongEdges, env);

    	// get the top track (most listened) for each user
    	DataSet<Tuple2<String, String>> usersWithTopTrack = userSongGraph.reduceOnEdges(
    			new EdgesFunction<String, Integer, String>() {
					public Tuple2<String, String> iterateEdges(
							Iterable<Tuple2<String, Edge<String, Integer>>> edges) {
						int maxPlaycount = 0;
						String userId = ""; 
						String topSong = "";

						final Iterator<Tuple2<String, Edge<String, Integer>>> edgesIterator = 
								edges.iterator();
						if (edgesIterator.hasNext()) {
							Tuple2<String, Edge<String, Integer>> first = edgesIterator.next();
							userId = first.f0;
							topSong = first.f1.getTarget();
						}
						while (edgesIterator.hasNext()) {
							Tuple2<String, Edge<String, Integer>> edge = edgesIterator.next();
							if (edge.f1.getValue() > maxPlaycount) {
								maxPlaycount = edge.f1.getValue();
								topSong = edge.f1.getTarget();
							}
						}
						return new Tuple2<String, String> (userId, topSong);
					}
		}, EdgeDirection.OUT);
    }

	@Override
	public String getDescription() {
		return null;
	}

}
