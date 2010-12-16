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

package eu.stratosphere.nephele.visualization.swt;

import java.util.Iterator;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;

import eu.stratosphere.nephele.topology.NetworkTopology;

public class SWTNetworkTopology extends AbstractSWTComponent {

	private final NetworkTopology networkTopology;

	private SWTNetworkNode rootNode;

	protected SWTNetworkTopology(NetworkTopology networkTopology) {
		super(null);

		this.networkTopology = networkTopology;
	}

	@Override
	public boolean isSelectable() {

		return false;
	}

	@Override
	public void layout() {

		final int depth = this.networkTopology.getDepth();
		if (depth == 0) {
			return;
		}
		final int[] nodesPerLevel = new int[depth];
		final int[] nodesLayoutedPerLevel = new int[depth];
		final int cellHeight = this.rect.height / depth;

		for (int i = 0; i < depth; i++) {
			nodesPerLevel[i] = this.rootNode.getNumberOfNodesOnLevel(i + 1);
			nodesLayoutedPerLevel[i] = 0; // Initialize the other data structure on that occasion
		}

		final Iterator<AbstractSWTComponent> it = getChildren();
		// The order of the iterator follows the order of {@link NetworkTopologyIterator}
		while (it.hasNext()) {

			final AbstractSWTComponent abstractSWTComponent = it.next();
			if (abstractSWTComponent instanceof SWTNetworkNode) {
				final SWTNetworkNode swtNetworkNode = (SWTNetworkNode) abstractSWTComponent;
				final int nodeDepth = swtNetworkNode.getDepth() - 1;
				final int cellWidth = this.rect.width / nodesPerLevel[nodeDepth];

				swtNetworkNode.setHeight(Math.min(cellHeight, SWTNetworkNode.MAXHEIGHT));
				swtNetworkNode.setWidth(Math.min(cellWidth, SWTNetworkNode.MAXWIDTH));
				// Calculate y position first
				int y = this.rect.y + nodeDepth * cellHeight;
				if (SWTNetworkNode.MAXHEIGHT < cellHeight) {
					y += (cellHeight - SWTNetworkNode.MAXHEIGHT) / 2;
				}
				swtNetworkNode.setY(y);

				int x = this.rect.x + (this.rect.width / 2) - (nodesPerLevel[nodeDepth] * cellWidth) / 2;
				x += (nodesLayoutedPerLevel[nodeDepth]++) * cellWidth;
				if (SWTNetworkNode.MAXWIDTH < cellWidth) {
					x += (cellWidth - SWTNetworkNode.MAXWIDTH) / 2;
				}
				swtNetworkNode.setX(x);
			}
		}

		layoutChildren();
	}

	public void setRootNode(SWTNetworkNode rootNode) {
		this.rootNode = rootNode;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void paintInternal(GC gc, Device device) {

		gc.setBackground(ColorScheme.getNetworkTopologyBackgroundColor(device));
		gc.fillRectangle(this.rect);
	}

}
