package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import eu.stratosphere.sopremo.sdaa11.util.FastStringComparator;

public class ClusterQueue {
	static class ClusterPair implements Comparable<ClusterPair> {
		private final HierarchicalCluster cluster1;
		private final HierarchicalCluster cluster2;
		private final int distance;
		private final int hash;

		public ClusterPair(final HierarchicalCluster cluster1,
				final HierarchicalCluster cluster2) {
			this.cluster1 = cluster1;
			this.cluster2 = cluster2;
			this.distance = cluster1.getClustroid().getDistance(
					cluster2.getClustroid());
			this.hash = cluster1.hashCode() ^ cluster2.hashCode() << 1;
		}

		public HierarchicalCluster getCluster1() {
			return this.cluster1;
		}

		public HierarchicalCluster getCluster2() {
			return this.cluster2;
		}

		public boolean contains(final HierarchicalCluster cluster) {
			return this.cluster1.equals(cluster)
					|| this.cluster2.equals(cluster);
		}

		int getDistance() {
			return this.distance;
		}

		@Override
		public int compareTo(final ClusterPair otherPair) {
			int difference = this.distance - otherPair.distance;
			if (difference != 0)
				return difference;

			// at this point, any criterion ordering the pairs is sufficient
			difference = FastStringComparator.INSTANCE.compare(
					this.cluster1.getId(), otherPair.cluster1.getId());
			if (difference != 0)
				return difference;
			return FastStringComparator.INSTANCE.compare(this.cluster2.getId(),
					otherPair.cluster2.getId());
		}

		@Override
		public String toString() {
			return "ClusterPair<" + this.cluster1 + " and " + this.cluster2
					+ " with distance " + this.distance + ">";
		}

		@Override
		public int hashCode() {
			return this.hash;
		}
	}

	private final Set<HierarchicalCluster> clusters = new HashSet<HierarchicalCluster>();
	private final SortedMap<Integer, Set<ClusterPair>> distancedPairs = new TreeMap<Integer, Set<ClusterPair>>();

	public void add(final HierarchicalCluster cluster) {
		for (final HierarchicalCluster otherCluster : this.clusters) {
			final ClusterPair pair = new ClusterPair(cluster, otherCluster);
			this.add(pair);
		}
		this.clusters.add(cluster);
	}

	public void removeCluster(final HierarchicalCluster cluster) {
		this.clusters.remove(cluster);
		this.removePairsWith(cluster);
	}

	private boolean add(final ClusterPair pair) {
		final int distance = pair.getDistance();
		Set<ClusterPair> set = this.distancedPairs.get(distance);
		if (set == null) {
			set = new HashSet<ClusterPair>();
			this.distancedPairs.put(distance, set);
		}
		return set.add(pair);
	}

	private void removePairsWith(final HierarchicalCluster cluster) {
		final Set<Integer> keysToRemove = new HashSet<Integer>();
		for (final Integer key : this.distancedPairs.keySet()) {
			final Set<ClusterPair> originalPairs = this.distancedPairs.get(key);
			final Set<ClusterPair> pairs = new HashSet<ClusterPair>(
					originalPairs);
			for (final ClusterPair clusterPair : pairs)
				if (clusterPair.contains(cluster))
					originalPairs.remove(clusterPair);
			if (originalPairs.isEmpty())
				keysToRemove.add(key);
		}

		for (final Integer key : keysToRemove)
			this.distancedPairs.remove(key);
	}

	public ClusterPair getFirstElement() {
		final Integer key = this.distancedPairs.firstKey();
		return this.distancedPairs.get(key).iterator().next();
	}

	public int getNumberOfClusters() {
		return this.clusters.size();
	}

	public Set<HierarchicalCluster> getClusters() {
		return this.clusters;
	}
}
