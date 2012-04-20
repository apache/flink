package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.ClusterQueue.ClusterPair;

public class InitialClusteringReducer extends ReduceStub {

	private final PactRecord outputRecord = new PactRecord();
	private final PactString clusterName = new PactString();
	private final PactPointList clusterPoints = new PactPointList();

	private ClusterQueue queue = new ClusterQueue();
	private final List<HierarchicalCluster> clusters = new ArrayList<HierarchicalCluster>();
	private int clusterCount = 0;

	@Override
	public void reduce(final Iterator<PactRecord> records, final Collector out)
			throws Exception {
		this.addPoints(records);
		this.cluster();
		this.emitClusters(out);
	}

	private void addPoints(final Iterator<PactRecord> records) {
		while (records.hasNext()) {
			final Point point = PointUtil.getPoint(records.next());
			this.add(point);
		}
	}

	private void add(final Point point) {
		this.queue.add(new BaseCluster(point, this.createNewId()));
	}

	private void cluster() {
		// Hierarchical clustering: Cluster until there is only one cluster
		// left.
		while (this.queue.getNumberOfClusters() > 1) {
			final ClusterPair pair = this.queue.getFirstElement();
			final HierarchicalCluster cluster1 = pair.getCluster1();
			final HierarchicalCluster cluster2 = pair.getCluster2();

			// System.out.println("Merging "+cluster1.getId()+" and "+cluster2.getId());

			final HierarchicalCluster mergedCluster = new MergedCluster(
					cluster1, cluster2, this.createNewId());
			this.queue.removeCluster(cluster1);
			this.queue.removeCluster(cluster2);

			// If the new cluster can still be a GRGPF cluster, we will not
			// consider its children anymore.
			final boolean makeFinal = this.canBeFinal(mergedCluster);
			mergedCluster.makeFinal(makeFinal);
			if (makeFinal)
				this.queue.add(mergedCluster);
			else
				for (final HierarchicalCluster child : mergedCluster
						.getChildren())
					this.clusters.add(child);
		}
		this.clusters.addAll(this.queue.getClusters());
		this.queue = null;
	}

	/**
	 * Tells whether the cluster can be used as cluster in GRGPF.<br>
	 * This method is to satisfy the following condition:<br>
	 * <i>!canBeFinal(c1) | !canBeFinal(c2) => !canBeFinal(c1+c2)</i>
	 */
	private boolean canBeFinal(final HierarchicalCluster cluster) {
		return cluster.canBeFinal()
				&& cluster.getRadius() < Settings.MAX_RADIUS
				&& cluster.size() < Settings.MAX_INITIAL_CLUSTER_SIZE;
	}

	private void emitClusters(final Collector out) {
		for (final HierarchicalCluster cluster : this.clusters)
			this.emit(cluster, out);
	}

	private void emit(final HierarchicalCluster cluster, final Collector out) {
		if (cluster.isFinal()) {
			this.clusterName.setValue(cluster.getId());
			this.clusterPoints.clear();
			this.clusterPoints.addAll(Arrays.asList(cluster.getPoints()));

			this.outputRecord.clear();
			this.outputRecord.setField(PactRecordIndices.CLUSTER_ID,
					this.clusterName);
			this.outputRecord.setField(PactRecordIndices.CLUSTER_POINTS,
					this.clusterPoints);
			this.outputRecord.setField(PactRecordIndices.CLUSTER_CLUSTROID,
					cluster.getClustroid());

			out.collect(this.outputRecord);
		} else
			for (final HierarchicalCluster child : cluster.getChildren())
				this.emit(child, out);
	}

	private String createNewId() {
		return String.valueOf(this.clusterCount++);
	}

}
