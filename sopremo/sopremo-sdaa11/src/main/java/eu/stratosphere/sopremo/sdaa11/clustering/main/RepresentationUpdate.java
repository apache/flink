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
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
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

	/**
	 * 
	 */
	private static final List<ObjectAccess> KEY_EXPRESSIONS = 
			Arrays.asList(new ObjectAccess("id"), new ObjectAccess("cluster"));

	/**
	 * 
	 */
	private static final long serialVersionUID = -5470882666744483986L;

	private int representationCount = 10;
	
	private int maxRadius = 500;
	
	private int maxClustroidShift = 200;
	
	private int minPointsForSplitting = 21;
	

	/**
	 * Returns the minPointsForSplitting.
	 * 
	 * @return the minPointsForSplitting
	 */
	public int getMinPointsForSplitting() {
		return minPointsForSplitting;
	}

	/**
	 * Sets the minPointsForSplitting to the specified value.
	 *
	 * @param minPointsForSplitting the minPointsForSplitting to set
	 */
	public void setMinPointsForSplitting(int minPointsForSplitting) {
		this.minPointsForSplitting = minPointsForSplitting;
	}

	/**
	 * Returns the maxDiameter.
	 * 
	 * @return the maxDiameter
	 */
	public int getMaxRadius() {
		return maxRadius;
	}

	/**
	 * Sets the maxDiameter to the specified value.
	 *
	 * @param maxDiameter the maxDiameter to set
	 */
	public void setMaxRadius(int maxDiameter) {
		this.maxRadius = maxDiameter;
	}

	/**
	 * Returns the maxClustroidShift.
	 * 
	 * @return the maxClustroidShift
	 */
	public int getMaxClustroidShift() {
		return maxClustroidShift;
	}

	/**
	 * Sets the maxClustroidShift to the specified value.
	 *
	 * @param maxClustroidShift the maxClustroidShift to set
	 */
	public void setMaxClustroidShift(int maxClustroidShift) {
		this.maxClustroidShift = maxClustroidShift;
	}

	/**
	 * Sets the representationCount to the specified value.
	 *
	 * @param representationCount the representationCount to set
	 */
	public void setRepresentationCount(int representationCount) {
		this.representationCount = representationCount;
	}

	/**
	 * Returns the representationCount.
	 * 
	 * @return the representationCount
	 */
	public int getRepresentationCount() {
		return this.representationCount;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions()
	 */
	@Override
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		System.out.println("Asked for key expression");
		return KEY_EXPRESSIONS;
	}

	public static class Implementation extends SopremoCoGroup {

		private int representationCount = 10;
		
		private int maxRadius = 500;
		
		private int maxClustroidShift = 200;
		
		private int minPointsForSplitting = 21;

		private final ObjectNode outputNode = new ObjectNode();
		
		private final TextNode idNode = new TextNode();
		private ObjectNode pointNode = new ObjectNode();
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
			
			String id = JsonUtil2.getField(representationNode, "id", TextNode.class).getJavaValue();
			Point oldClustroid = new Point();
			oldClustroid.read(representationNode.get("clustroid"));
			
			// TODO: Evaluate whether starting with an empty representation yields better results
			ClusterRepresentation representation = 
					new ClusterRepresentation(id, oldClustroid, representationCount);

			for (final IJsonNode memberNode : pointsNode) {
				final Point point = new Point();
				point.read(memberNode);
				representation.add(point);
			}
			
			emitRepresentation(representation, oldClustroid, out);

		}

		private void emitRepresentation(ClusterRepresentation representation, Point oldClustroid, JsonCollector out) {
			if (shallSplit(representation)) {
				emitSplitRepresentations(representation, out);
			} else if (shallRecluster(representation, oldClustroid)) {
				emitUnstableRepresentation(representation, out);
			} else {
				emitStableRepresentation(representation, out);
			}
		}


		private boolean shallSplit(ClusterRepresentation representation) {
			return representation.size() >= minPointsForSplitting
						&& representation.getRadius() >= maxRadius;
		}
		
		private void emitSplitRepresentations(ClusterRepresentation representation, JsonCollector out) {
			// TODO: Splitting might work in the future in-place
			String oldID = representation.getId();
			Point[] newClustroids = representation.findSplittingClustroids();
			
			emit(oldID+"a", newClustroids[0], ClusterRepresentation.SPLIT_FLAG, oldID, out);
			emit(oldID+"b", newClustroids[1], ClusterRepresentation.SPLIT_FLAG, oldID, out);
		}

		private boolean shallRecluster(ClusterRepresentation representation, Point oldClustroid) {
			return oldClustroid.getDistance(representation.getClustroid()) >= maxClustroidShift;
		}

		private void emitUnstableRepresentation(ClusterRepresentation representation, JsonCollector out) {
			emit(
					representation.getId(), 
					representation.getClustroid(), 
					ClusterRepresentation.RECLUSTER_FLAG, 
					representation.getId(),
					out);
		}

		private void emitStableRepresentation(ClusterRepresentation representation, JsonCollector out) {
			emit(
					representation.getId(), 
					representation.getClustroid(), 
					ClusterRepresentation.STABLE_FLAG, 
					representation.getId(),
					out);		
		}

		private void emit(String id, Point clustroid, int flag, String oldId, JsonCollector collector) {
			outputNode.clear();

			if (clustroid == null) {
				throw new IllegalArgumentException("Clustroid null in "+id);
			}
			
			idNode.setValue(id);
			outputNode.put("id", idNode);

			outputNode.put("clustroid", clustroid.write(pointNode));

			flagNode.setValue(flag);
			outputNode.put("flag", flagNode);

			oldIdNode.setValue(oldId);
			outputNode.put("oldId", oldIdNode);
			
			collector.collect(outputNode);
		}
	}

}
