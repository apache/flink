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
package eu.stratosphere.sopremo.sdaa11.clustering.postprocessing;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.json.RepresentationNodes;
import eu.stratosphere.sopremo.sdaa11.clustering.main.ClusterRepresentation;
import eu.stratosphere.sopremo.sdaa11.clustering.main.RepresentationUpdate;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * <ol>
 * <li>Representations</li>
 * <li>Points</li>
 * </ol>
 * 
 * @author skruse
 * 
 */
@InputCardinality(min = 2, max = 2)
public class Split extends ElementaryOperator<Split> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4746027973622561627L;
	private int representationDetail = RepresentationUpdate.DEFAULT_REPRESENTATION_DETAIL;

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

	public static class Implementation extends SopremoCoGroup {

		int representationDetail;
		private ClusterRepresentation representation2;
		private ClusterRepresentation representation1;
		private String parentId;

		private final ObjectNode outputNode = new ObjectNode();
		private final ObjectNode clustroidNode = new ObjectNode();
		private final TextNode idNode = new TextNode();
		private final TextNode parentIdNode = new TextNode();
		private final IntNode flagNode = new IntNode();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere
		 * .sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void coGroup(final IArrayNode representationNodes,
				final IArrayNode pointNodes, final JsonCollector out) {
			if (representationNodes.size() != 2)
				throw new IllegalArgumentException(
						"Expected 2 representations, found "
								+ representationNodes.size());
			final ObjectNode representationNode1 = (ObjectNode) representationNodes
					.get(0);
			final ObjectNode representationNode2 = (ObjectNode) representationNodes
					.get(1);
			this.representation1 = RepresentationNodes.read(
					representationNode1, this.representationDetail);
			this.representation2 = RepresentationNodes.read(
					representationNode2, this.representationDetail);
			this.parentId = RepresentationNodes
					.getParentId(representationNode1).getTextValue();

			this.addAll(pointNodes);
			this.emitRepresentations(out);

		}

		private void addAll(final IArrayNode pointNodes) {
			int dist1, dist2;
			boolean drawToggle = false;
			for (final IJsonNode pointNode : pointNodes) {
				final Point point = new Point();
				point.read(pointNode);
				dist1 = this.representation1.getClustroid().getDistance(point);
				dist2 = this.representation2.getClustroid().getDistance(point);
				if (dist1 <= dist2)
					this.representation1.add(point);
				else if (dist1 > dist2)
					this.representation2.add(point);
				else if (drawToggle ^= true)
					this.representation1.add(point);
				else
					this.representation2.add(point);
			}
		}

		private void emitRepresentations(final JsonCollector collector) {
			this.emit(this.representation1, collector);
			this.emit(this.representation2, collector);
		}

		private void emit(final ClusterRepresentation representation,
				final JsonCollector collector) {
			this.emit(representation.getId(), representation.getClustroid(),
					ClusterRepresentation.UNSTABLE_FLAG, this.parentId,
					collector);
		}

		private void emit(final String id, final Point clustroid,
				final int flag, final String parentId,
				final JsonCollector collector) {
			clustroid.write(this.clustroidNode);
			this.idNode.setValue(id);
			this.parentIdNode.setValue(parentId);
			this.flagNode.setValue(flag);
			RepresentationNodes.write(this.outputNode, this.idNode,
					this.parentIdNode, this.clustroidNode);
			RepresentationNodes.setFlag(this.outputNode, this.flagNode);
			collector.collect(this.outputNode);
		}
	}

}
