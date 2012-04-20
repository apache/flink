package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import eu.stratosphere.sopremo.sdaa11.util.FastStringComparator;



public class ClusterQueue {
	static class ClusterPair implements Comparable<ClusterPair> {
		private HierarchicalCluster cluster1;
		private HierarchicalCluster cluster2;
		private int distance;
		private int hash;
		
		public ClusterPair(HierarchicalCluster cluster1, HierarchicalCluster cluster2) {
			this.cluster1 = cluster1;
			this.cluster2 = cluster2;
			distance = cluster1.getClustroid().getDistance(cluster2.getClustroid());
			hash = cluster1.hashCode() ^ cluster2.hashCode() << 1;
		}
		
		public HierarchicalCluster getCluster1() {
			return cluster1;
		}
		
		public HierarchicalCluster getCluster2() {
			return cluster2;
		}
		
		public boolean contains(HierarchicalCluster cluster) {
			return cluster1.equals(cluster) || cluster2.equals(cluster);
		}
		
		int getDistance() {
			return distance;
		}
	
		public int compareTo(ClusterPair otherPair) {
			int difference = distance - otherPair.distance;
			if (difference != 0) return difference;
			
			// at this point, any criterion ordering the pairs is sufficient
			difference = FastStringComparator.INSTANCE.compare(cluster1.getId(), otherPair.cluster1.getId());
			if (difference != 0) return difference;
			return FastStringComparator.INSTANCE.compare(cluster2.getId(), otherPair.cluster2.getId());
		}
		
		@Override
		public String toString() {
			return "ClusterPair<" + cluster1 + " and " + cluster2 + " with distance " + distance + ">";
		}
		
		@Override
		public int hashCode() {
			return hash;
		}
	}

	private Set<HierarchicalCluster> clusters = new HashSet<HierarchicalCluster>();
	private SortedMap<Integer, Set<ClusterPair>> distancedPairs = new TreeMap<Integer, Set<ClusterPair>>();
	
	public void add(HierarchicalCluster cluster) {
		for (HierarchicalCluster otherCluster : clusters) {
			ClusterPair pair = new ClusterPair(cluster, otherCluster);
			add(pair);
		}
		clusters.add(cluster);
	}
	
	public void removeCluster(HierarchicalCluster cluster) {
		clusters.remove(cluster);
		removePairsWith(cluster);
	}
	
	private boolean add(ClusterPair pair) {
		int distance = pair.getDistance();
		Set<ClusterPair> set = distancedPairs.get(distance);
		if (set == null) {
			set = new HashSet<ClusterPair>();
			distancedPairs.put(distance, set);
		}
		return set.add(pair);
	}
	
	private void removePairsWith(HierarchicalCluster cluster) {
		Set<Integer> keysToRemove = new HashSet<Integer>();
		for (Integer key : distancedPairs.keySet()) {
			Set<ClusterPair> originalPairs = distancedPairs.get(key);
			Set<ClusterPair> pairs = new HashSet<ClusterPair>(originalPairs);
			for (ClusterPair clusterPair : pairs) {
				if (clusterPair.contains(cluster)) {
					originalPairs.remove(clusterPair);
				}
			}
			if (originalPairs.isEmpty()) {
				keysToRemove.add(key);
			}
		}
		
		for (Integer key : keysToRemove) {
			distancedPairs.remove(key);
		}
	}
	
	public ClusterPair getFirstElement() {
		Integer key = distancedPairs.firstKey();
		return distancedPairs.get(key).iterator().next();
	}
	
	public int getNumberOfClusters() {
		return clusters.size();
	}
	
	public Set<HierarchicalCluster> getClusters() {
		return clusters;
	}
}
