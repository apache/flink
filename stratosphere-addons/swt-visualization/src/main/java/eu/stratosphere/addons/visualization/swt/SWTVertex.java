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

package eu.stratosphere.addons.visualization.swt;

import java.util.Iterator;

import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class SWTVertex extends AbstractSWTVertex {

	private final static int SPACEFORGATESINPERCENT = 20;

	private int gateHeight = 0;

	private final ManagementVertex managementVertex;

	public SWTVertex(AbstractSWTComponent parent, ManagementVertex managementVertex) {
		super(parent);

		this.managementVertex = managementVertex;
	}

	@Override
	public void layout() {

		int numberOfInputGates = 0;
		int numberOfOutputGates = 0;

		Iterator<AbstractSWTComponent> it = getChildren();
		while (it.hasNext()) {

			AbstractSWTComponent child = it.next();
			if (child instanceof SWTGate) {
				SWTGate gate = (SWTGate) child;
				if (gate.isInputGate()) {
					numberOfInputGates++;
				} else {
					numberOfOutputGates++;
				}
			}
		}

		int numberOfLayoutedInputGates = 0;
		int numberOfLayoutedOutputGates = 0;
		int inputGateWidth = 0;
		if (numberOfInputGates > 0) {
			inputGateWidth = getWidth() / numberOfInputGates;
		}
		int outputGateWidth = 0;
		if (numberOfOutputGates > 0) {
			outputGateWidth = getWidth() / numberOfOutputGates;
		}

		this.gateHeight = (int) ((double) getHeight() * (SPACEFORGATESINPERCENT / 100.0f));
		it = getChildren();
		while (it.hasNext()) {

			AbstractSWTComponent child = it.next();
			if (child instanceof SWTGate) {
				SWTGate gate = (SWTGate) child;
				gate.setHeight(this.gateHeight);
				if (gate.isInputGate()) {
					gate.setX(getX() + numberOfLayoutedInputGates * inputGateWidth);
					gate.setY(getY() + getHeight() - this.gateHeight);
					gate.setWidth(inputGateWidth);

					numberOfLayoutedInputGates++;
				} else {
					gate.setX(getX() + numberOfLayoutedOutputGates * outputGateWidth);
					gate.setY(getY());
					gate.setWidth(outputGateWidth);

					numberOfLayoutedOutputGates++;
				}
			}

		}
	}

	@Override
	public boolean isSelectable() {
		return true;
	}

	@Override
	protected void paintInternal(final GC gc, final Device device) {

		final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) this.managementVertex
			.getGroupVertex().getAttachment();

		if (groupVertexVisualizationData.isCPUBottleneck()) {

			gc.setBackground(getBackgroundColor(device));
		} else {
			gc.setBackground(getBackgroundColor(device));
		}

		gc.fillRectangle(this.rect.x, this.rect.y, this.rect.width, this.rect.height);
	}

	private Color getBackgroundColor(Device device) {

		Color returnColor = null;

		switch (this.managementVertex.getExecutionState()) {
		case RUNNING:
			returnColor = ColorScheme.getVertexRunningBackgroundColor(device);
			break;
		case FINISHING:
			returnColor = ColorScheme.getVertexFinishingBackgroundColor(device);
			break;
		case FINISHED:
			returnColor = ColorScheme.getVertexFinishedBackgroundColor(device);
			break;
		case CANCELING:
		case CANCELED:
			returnColor = ColorScheme.getVertexCancelBackgroundColor(device);
			break;
		case FAILED:
			returnColor = ColorScheme.getVertexFailedBackgroundColor(device);
			break;
		default:
			returnColor = ColorScheme.getVertexDefaultBackgroundColor(device);
			break;
		}

		return returnColor;
	}

	@Override
	public SWTToolTip constructToolTip(Shell parentShell, SWTToolTipCommandReceiver commandReceiver, int x, int y) {

		return new SWTVertexToolTip(parentShell, commandReceiver, this.managementVertex, x, y);
	}
}
