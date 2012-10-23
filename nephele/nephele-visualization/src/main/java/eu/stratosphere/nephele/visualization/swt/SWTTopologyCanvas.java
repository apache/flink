/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

public class SWTTopologyCanvas extends Canvas implements PaintListener, Listener, MouseMoveListener,
		SWTToolTipCommandReceiver {

	private final SWTNetworkTopology swtNetworkTopology;

	private AbstractSWTComponent toolTipComponent = null;

	private SWTToolTip displayedToolTip = null;

	private final SWTJobTabItem jobTabItem;

	public SWTTopologyCanvas(GraphVisualizationData visualizationData, SWTJobTabItem jobTabItem, Composite parent,
			int style) {
		super(parent, style);

		this.jobTabItem = jobTabItem;
		this.swtNetworkTopology = createSWTNetworkTopology(visualizationData.getNetworkTopology());

		addListener(SWT.Resize, this);
		addPaintListener(this);
		addMouseMoveListener(this);
	}

	private SWTNetworkTopology createSWTNetworkTopology(NetworkTopology networkTopology) {

		final SWTNetworkTopology swtNetworkTopology = new SWTNetworkTopology(networkTopology);
		final Map<NetworkNode, SWTNetworkNode> nodeMap = new HashMap<NetworkNode, SWTNetworkNode>();

		final Iterator<NetworkNode> it = networkTopology.iterator();
		while (it.hasNext()) {

			final NetworkNode networkNode = it.next();
			final SWTNetworkNode swtNetworkNode = new SWTNetworkNode(getDisplay(), swtNetworkTopology, networkNode);
			swtNetworkTopology.addChild(swtNetworkNode);
			nodeMap.put(networkNode, swtNetworkNode);
			if (networkNode.getParentNode() == null) {
				swtNetworkTopology.setRootNode(swtNetworkNode);
			}
		}

		final Iterator<Map.Entry<NetworkNode, SWTNetworkNode>> it2 = nodeMap.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<NetworkNode, SWTNetworkNode> entry = it2.next();
			final NetworkNode childNode = entry.getKey();
			final NetworkNode parentNode = childNode.getParentNode();
			if (parentNode != null) {
				final SWTNetworkNode swtChildNode = entry.getValue();
				final SWTNetworkNode swtParentNode = nodeMap.get(parentNode);
				swtChildNode.setParentNode(swtParentNode);
			}
		}

		return swtNetworkTopology;
	}

	public void updateView() {

		// Redraw the graph pane
		redraw();

		// Update tooltip if there is one
		if (this.displayedToolTip != null) {
			if (!this.displayedToolTip.isDisposed()) {
				this.displayedToolTip.updateView();
			}
		}
	}

	@Override
	public void paintControl(PaintEvent arg0) {

		// Paint the topology
		this.swtNetworkTopology.paint(arg0.gc, getDisplay());
	}

	@Override
	public void handleEvent(Event arg0) {

		if (this.swtNetworkTopology != null) {

			final Rectangle rect = getClientArea();
			this.swtNetworkTopology.setX(rect.x);
			this.swtNetworkTopology.setY(rect.y);
			this.swtNetworkTopology.setWidth(rect.width);
			this.swtNetworkTopology.setHeight(rect.height);
			this.swtNetworkTopology.layout();
		}
	}

	@Override
	public void mouseMove(MouseEvent arg0) {

		final AbstractSWTComponent selectedComponent = this.swtNetworkTopology.getSelectedComponent(arg0.x, arg0.y);
		if (selectedComponent == null) {
			if (this.toolTipComponent != null) {
				this.displayedToolTip.dispose();
				this.displayedToolTip = null;
				this.toolTipComponent = null;
			}
		} else {

			final Point clientPoint = this.toDisplay(arg0.x, arg0.y);

			// selectedComponent != null
			if (selectedComponent == toolTipComponent) {
				if (this.displayedToolTip != null) {
					this.displayedToolTip.move(clientPoint.x, clientPoint.y);
				}
			} else {
				// A new component
				if (this.displayedToolTip != null && !this.displayedToolTip.isDisposed()) {
					this.displayedToolTip.dispose();
				}

				this.displayedToolTip = selectedComponent.constructToolTip(getShell(), this, clientPoint.x,
					clientPoint.y);
				if (displayedToolTip != null) {
					this.displayedToolTip.move(clientPoint.x, clientPoint.y);
					this.toolTipComponent = selectedComponent;
				} else {
					this.toolTipComponent = null;
				}
			}
		}
	}

	@Override
	public void killTask(final ManagementVertexID id, final String taskName) {
		// Nothing to do here
	}

	@Override
	public void killInstance(final String instanceName) {

		this.jobTabItem.killInstance(instanceName);
	}

	@Override
	public void switchToInstance(final String instanceName) {
		// Nothing to do here
	}

}
