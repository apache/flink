package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.sdaa11.Annotator;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.ClusterQueue.ClusterPair;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SequentialClustering extends ElementaryOperator<SequentialClustering> {
	
	private static final long serialVersionUID = 5563265035325926095L;

	/** The maximum radius of a cluster. */
	private int maxRadius;
	
	/** The maximum number of points of a cluster. */
	private int maxSize;
	
	public int getMaxRadius() {
		return maxRadius;
	}

	public void setMaxRadius(int maxRadius) {
		this.maxRadius = maxRadius;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	
	@Override
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		return Arrays.asList(new ArrayAccess(Annotator.DUMMY_VALUE_INDEX));
	}
	
	public static class Implementation extends SopremoReduce {
		
		private int maxRadius;
		private int maxSize;

		private ClusterQueue queue = new ClusterQueue();
		private List<HierarchicalCluster> clusters = new ArrayList<HierarchicalCluster>();
		private int idCounter = 0;
		
		@Override
		protected void reduce(IArrayNode values, JsonCollector out) {
			addPoints(values);
			cluster();
			emitClusters(out);
		}

		private void addPoints(IArrayNode values) {
			for (IJsonNode value : values) {
				Point point = null; //new Point(value);
				queue.add(new BaseCluster(point, String.valueOf(createNewId())));
			}
		}

		private void cluster() {
			// Hierarchical clustering: Cluster until there is only one cluster left.
			while (queue.getNumberOfClusters() > 1) {
				ClusterPair pair = queue.getFirstElement();
				HierarchicalCluster cluster1 = pair.getCluster1();
				HierarchicalCluster cluster2 = pair.getCluster2();
				
				HierarchicalCluster mergedCluster = new MergedCluster(cluster1, cluster2, createNewId());
				queue.removeCluster(cluster1);
				queue.removeCluster(cluster2);
				
				// If the new cluster can be a final cluster, we will not
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
			return cluster.canBeFinal() && cluster.getRadius() < maxRadius 
					&& cluster.size() < maxSize;
		}

		private void emitClusters(JsonCollector out) {
			for (HierarchicalCluster cluster : clusters) {
				emit(cluster, out);
			}
		}

		private void emit(HierarchicalCluster cluster, JsonCollector out) {
			if (cluster.isFinal()) {
				ArrayNode pointsNode = new ArrayNode();
				for (Point point : cluster.getPoints()) {
					pointsNode.add(point.toJsonNode(null));
				}
				
				ObjectNode clusterNode = new ObjectNode();
				clusterNode.put("id", new TextNode(cluster.getId()));
				clusterNode.put("clustroid", cluster.getClustroid().toJsonNode(null));
				clusterNode.put("points", pointsNode);
				
				out.collect(clusterNode);
			} else {
				for (HierarchicalCluster child : cluster.getChildren()) {
					emit(child, out);
				}
			}
		}

		private String createNewId() {
			return String.valueOf(idCounter++);
		}
	}

}
