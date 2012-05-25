/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.sdaa11.clustering.main;

import java.util.Arrays;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.json.PointNodes;
import eu.stratosphere.sopremo.sdaa11.clustering.json.RepresentationNodes;
import eu.stratosphere.sopremo.sdaa11.util.JsonUtil2;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
@InputCardinality(min = 2, max = 2)
public class RepresentationUpdate extends
		ElementaryOperator<RepresentationUpdate> {

	private static final long serialVersionUID = -5470882666744483986L;

	public static final int DEFAULT_MIN_POINT_COUNT = 21;

	public static final int DEFAULT_MAX_CLUSTROID_SHIFT = 200;

	public static final int DEFAULT_MAX_CLUSTER_RADIUS = 500;

	public static final int DEFAULT_REPRESENTATION_DETAIL = 10;

	private int representationDetail = DEFAULT_REPRESENTATION_DETAIL;

	private int maxClusterRadius = DEFAULT_MAX_CLUSTER_RADIUS;

	private int maxClustroidShift = DEFAULT_MAX_CLUSTROID_SHIFT;

	private int minPointCount = DEFAULT_MIN_POINT_COUNT;

	/**
	 * Returns the representationDetail.
	 * 
	 * @return the representationDetail
	 */
	public int getRepresentationDetail() {
		return this.representationDetail;
	}

	/**
	 * Sets the representationDetail to the specified value.
	 * 
	 * @param representationDetail
	 *            the representationDetail to set
	 */
	public void setRepresentationDetail(final int representationDetail) {
		this.representationDetail = representationDetail;
	}

	/**
	 * Returns the maxClusterRadius.
	 * 
	 * @return the maxClusterRadius
	 */
	public int getMaxClusterRadius() {
		return this.maxClusterRadius;
	}

	/**
	 * Sets the maxClusterRadius to the specified value.
	 * 
	 * @param maxClusterRadius
	 *            the maxClusterRadius to set
	 */
	public void setMaxClusterRadius(final int maxClusterRadius) {
		this.maxClusterRadius = maxClusterRadius;
	}

	/**
	 * Returns the maxClustroidShift.
	 * 
	 * @return the maxClustroidShift
	 */
	public int getMaxClustroidShift() {
		return this.maxClustroidShift;
	}

	/**
	 * Sets the maxClustroidShift to the specified value.
	 * 
	 * @param maxClustroidShift
	 *            the maxClustroidShift to set
	 */
	public void setMaxClustroidShift(final int maxClustroidShift) {
		this.maxClustroidShift = maxClustroidShift;
	}

	/**
	 * Returns the minPointCount.
	 * 
	 * @return the minPointCount
	 */
	public int getMinPointCount() {
		return this.minPointCount;
	}

	/**
	 * Sets the minPointCount to the specified value.
	 * 
	 * @param minPointCount
	 *            the minPointCount to set
	 */
	public void setMinPointCount(final int minPointCount) {
		this.minPointCount = minPointCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions()
	 */
	@Override
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		// return ALL_KEYS;
		return Arrays
				.asList(new PathExpression(new InputSelection(0),
						new ObjectAccess(RepresentationNodes.ID)),
						new PathExpression(new InputSelection(1),
								new ObjectAccess(PointNodes.CLUSTER_ID)));
	}

	public static class Implementation extends SopremoCoGroup {

		private int representationDetail;

		private int maxClusterRadius;

		private int maxClustroidShift;

		private int minPointCount;

		private final ObjectNode outputNode = new ObjectNode();

		private final TextNode idNode = new TextNode();
		private final ObjectNode pointNode = new ObjectNode();
		private final IntNode flagNode = new IntNode();
		private final TextNode oldIdNode = new TextNode();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere
		 * .sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void coGroup(final IArrayNode representationsNode,
				final IArrayNode pointsNode, final JsonCollector out) {

			if (representationsNode.size() != 1)
				throw new IllegalStateException(
						"Unexpected number of representations: "
								+ representationsNode.size());
			final ObjectNode representationNode = (ObjectNode) representationsNode
					.get(0);

			final String id = JsonUtil2.getField(representationNode, "id",
					TextNode.class).getJavaValue();
			final Point oldClustroid = new Point();
			oldClustroid.read(representationNode.get("clustroid"));

			// TODO: Evaluate whether starting with an empty representation
			// yields better results
			final ClusterRepresentation representation = new ClusterRepresentation(
					id, oldClustroid, this.representationDetail);

			for (final IJsonNode memberNode : pointsNode) {
				final Point point = new Point();
				point.read(memberNode);
				representation.add(point);
			}

			this.emitRepresentation(representation, oldClustroid, out);

		}

		private void emitRepresentation(
				final ClusterRepresentation representation,
				final Point oldClustroid, final JsonCollector out) {
			if (this.shallSplit(representation))
				this.emitSplitRepresentations(representation, out);
			else if (this.shallRecluster(representation, oldClustroid))
				this.emitUnstableRepresentation(representation, out);
			else
				this.emitStableRepresentation(representation, out);
		}

		private boolean shallSplit(final ClusterRepresentation representation) {
			return representation.size() >= this.minPointCount
					&& representation.getRadius() >= this.maxClusterRadius;
		}

		private void emitSplitRepresentations(
				final ClusterRepresentation representation,
				final JsonCollector out) {
			// TODO: Splitting might work in the future in-place
			final String oldID = representation.getId();
			final Point[] newClustroids = representation
					.findSplittingClustroids();

			this.emit(oldID + "a", newClustroids[0],
					ClusterRepresentation.SPLIT_FLAG, oldID, out);
			this.emit(oldID + "b", newClustroids[1],
					ClusterRepresentation.SPLIT_FLAG, oldID, out);
		}

		private boolean shallRecluster(
				final ClusterRepresentation representation,
				final Point oldClustroid) {
			return oldClustroid.getDistance(representation.getClustroid()) >= this.maxClustroidShift;
		}

		private void emitUnstableRepresentation(
				final ClusterRepresentation representation,
				final JsonCollector out) {
			this.emit(representation.getId(), representation.getClustroid(),
					ClusterRepresentation.RECLUSTER_FLAG,
					representation.getId(), out);
		}

		private void emitStableRepresentation(
				final ClusterRepresentation representation,
				final JsonCollector out) {
			this.emit(representation.getId(), representation.getClustroid(),
					ClusterRepresentation.STABLE_FLAG, representation.getId(),
					out);
		}

		private void emit(final String id, final Point clustroid,
				final int flag, final String oldId,
				final JsonCollector collector) {
			this.outputNode.clear();

			if (clustroid == null)
				throw new IllegalArgumentException("Clustroid null in " + id);

			this.idNode.setValue(id);
			this.outputNode.put("id", this.idNode);

			this.outputNode.put("clustroid", clustroid.write(this.pointNode));

			this.flagNode.setValue(flag);
			this.outputNode.put("flag", this.flagNode);

			this.oldIdNode.setValue(oldId);
			this.outputNode.put("oldId", this.oldIdNode);

			collector.collect(this.outputNode);
		}
	}

}
