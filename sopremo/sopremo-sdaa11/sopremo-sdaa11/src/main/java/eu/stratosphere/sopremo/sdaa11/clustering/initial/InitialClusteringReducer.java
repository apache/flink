package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import de.hpi.fgis.sdaa11.grgpf.stratosphere.Settings;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.data.PactPointList;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.data.PactRecordIndices;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.data.Point;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.initialclustering.ClusterQueue.ClusterPair;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.initialclustering.cluster.BaseCluster;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.initialclustering.cluster.HierarchicalCluster;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.initialclustering.cluster.MergedCluster;
import de.hpi.fgis.sdaa11.grgpf.stratosphere.util.PointUtil;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;


public class InitialClusteringReducer extends ReduceStub {
	
	private PactRecord outputRecord = new PactRecord();
	private PactString clusterName = new PactString();
	private PactPointList clusterPoints = new PactPointList();
	
	private ClusterQueue queue = new ClusterQueue();
	private List<HierarchicalCluster> clusters = new ArrayList<HierarchicalCluster>();
	private int clusterCount = 0;
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector out)
			throws Exception {
		addPoints(records);
		cluster();
		emitClusters(out);
	}
	
	private void addPoints(Iterator<PactRecord> records) {
		while (records.hasNext()) {
			Point point = PointUtil.getPoint(records.next());
			add(point);
		}
	}
	
	private void add(Point point) {
		queue.add(new BaseCluster(point, createNewId()));
	}

	private void cluster() {
		// Hierarchical clustering: Cluster until there is only one cluster left.
		while (queue.getNumberOfClusters() > 1) {
			ClusterPair pair = queue.getFirstElement();
			HierarchicalCluster cluster1 = pair.getCluster1();
			HierarchicalCluster cluster2 = pair.getCluster2();
			
			// System.out.println("Merging "+cluster1.getId()+" and "+cluster2.getId());
			
			HierarchicalCluster mergedCluster = new MergedCluster(cluster1, cluster2, createNewId());
			queue.removeCluster(cluster1);
			queue.removeCluster(cluster2);
			
			// If the new cluster can still be a GRGPF cluster, we will not
			// consider its children anymore.
			boolean makeFinal = canBeFinal(mergedCluster);
			mergedCluster.makeFinal(makeFinal);
			if (makeFinal) {
				queue.add(mergedCluster);
			} else {
				for (HierarchicalCluster child : mergedCluster.getChildren()) {
					clusters.add(child);
				}
			}
		}
		clusters.addAll(queue.getClusters());
		queue = null;
	}

	/**
	 * Tells whether the cluster can be used as cluster in GRGPF.<br>
	 * This method is to satisfy the following condition:<br>
	 * <i>!canBeFinal(c1) | !canBeFinal(c2) => !canBeFinal(c1+c2)</i>
	 */
	private boolean canBeFinal(HierarchicalCluster cluster) {
		return cluster.canBeFinal() && cluster.getRadius() < Settings.MAX_RADIUS 
				&& cluster.size() < Settings.MAX_INITIAL_CLUSTER_SIZE;
	}

	private void emitClusters(Collector out) {
		for (HierarchicalCluster cluster : clusters) {
			emit(cluster, out);
		}
	}

	private void emit(HierarchicalCluster cluster, Collector out) {
		if (cluster.isFinal()) {
				clusterName.setValue(cluster.getId());
				clusterPoints.clear();
				clusterPoints.addAll(Arrays.asList(cluster.getPoints()));
				
				outputRecord.clear();
				outputRecord.setField(PactRecordIndices.CLUSTER_ID, clusterName);
				outputRecord.setField(PactRecordIndices.CLUSTER_POINTS, clusterPoints);
				outputRecord.setField(PactRecordIndices.CLUSTER_CLUSTROID, cluster.getClustroid());
				
				out.collect(outputRecord);
		} else {
			for (HierarchicalCluster child : cluster.getChildren()) {
				emit(child, out);
			}
		}
	}

	private String createNewId() {
		return String.valueOf(clusterCount++);
	}
	
	
}
