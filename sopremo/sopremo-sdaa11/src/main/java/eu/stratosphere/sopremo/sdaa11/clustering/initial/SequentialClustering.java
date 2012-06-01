package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.ClusterQueue.ClusterPair;
import eu.stratosphere.sopremo.sdaa11.clustering.json.ClusterNodes;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SequentialClustering extends
		ElementaryOperator<SequentialClustering> {

	private static final long serialVersionUID = 5563265035325926095L;

	public static final int DEFAULT_MAX_RADIUS = 500;
	public static final int DEFAULT_MAX_SIZE = 1000;

	/** The maximum radius of a cluster. */
	private int maxRadius = DEFAULT_MAX_RADIUS;

	/** The maximum number of points of a cluster. */
	private int maxSize = DEFAULT_MAX_SIZE;

	public int getMaxRadius() {
		return this.maxRadius;
	}

	public void setMaxRadius(final int maxRadius) {
		this.maxRadius = maxRadius;
	}

	public int getMaxSize() {
		return this.maxSize;
	}

	public void setMaxSize(final int maxSize) {
		this.maxSize = maxSize;
	}

	@Override
	public List<? extends EvaluationExpression> getKeyExpressions(
			final int inputIndex) {
		if (inputIndex != 0)
			throw new IllegalArgumentException("Illegal input index: "
					+ inputIndex);
		return Arrays.asList(new ObjectAccess(AnnotatorNodes.ANNOTATION));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#toString()
	 */
	@Override
	public String toString() {
		return "SequentialClustering[maxRadius=" + this.maxRadius + ";maxSize"
				+ this.maxSize + "]";
	}

	public static class Implementation extends SopremoReduce {

		private int maxRadius;
		private int maxSize;

		private ClusterQueue queue = new ClusterQueue();
		private final List<HierarchicalCluster> clusters = new ArrayList<HierarchicalCluster>();
		private int idCounter = 0;

		private final ObjectNode outputNode = new ObjectNode();
		private final TextNode idNode = new TextNode();
		private final IArrayNode pointsNode = new ArrayNode();
		private final ObjectNode clustroidNode = new ObjectNode();

		@Override
		protected void reduce(final IArrayNode values, final JsonCollector out) {

			System.out.println("Sequential clustering: " + values);

			this.addPoints(values);
			this.cluster();
			this.emitClusters(out);
		}

		private void addPoints(final IArrayNode values) {
			for (final IJsonNode value : values) {
				final Point point = new Point();
				point.read(AnnotatorNodes.getAnnotatee((ObjectNode) value));
				this.queue.add(new BaseCluster(point, String.valueOf(this
						.createNewId())));
			}
		}

		private void cluster() {
			// Hierarchical clustering: Cluster until there is only one cluster
			// left.
			while (this.queue.getNumberOfClusters() > 1) {
				final ClusterPair pair = this.queue.getFirstElement();
				if (pair.getDistance() > this.maxRadius)
					break;
				final HierarchicalCluster cluster1 = pair.getCluster1();
				final HierarchicalCluster cluster2 = pair.getCluster2();

				final HierarchicalCluster mergedCluster = new MergedCluster(
						cluster1, cluster2, this.createNewId());
				this.queue.removeCluster(cluster1);
				this.queue.removeCluster(cluster2);

				// If the new cluster can be a final cluster, we will not
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
			return cluster.canBeFinal() && cluster.getRadius() < this.maxRadius
					&& cluster.size() < this.maxSize;
		}

		private void emitClusters(final JsonCollector out) {
			for (final HierarchicalCluster cluster : this.clusters)
				this.emit(cluster, out);
		}

		private void emit(final HierarchicalCluster cluster,
				final JsonCollector out) {
			if (cluster.isFinal()) {
				this.pointsNode.clear();
				for (final Point point : cluster.getPoints())
					this.pointsNode.add(point.write((IJsonNode) null));

				this.idNode.setValue(cluster.getId());

				cluster.getClustroid().write(this.clustroidNode);

				ClusterNodes.write(this.outputNode, this.idNode,
						this.clustroidNode, this.pointsNode);
				out.collect(this.outputNode);
			} else
				for (final HierarchicalCluster child : cluster.getChildren())
					this.emit(child, out);
		}

		private String createNewId() {
			return String.valueOf(this.idCounter++);
		}
	}

}
