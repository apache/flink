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

import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;

public class SWTGate extends AbstractSWTVertex {

	private final ManagementGate mangementGate;

	protected SWTGate(AbstractSWTComponent parent, ManagementGate managementGate) {
		super(parent);

		this.mangementGate = managementGate;
	}

	@Override
	public boolean isSelectable() {

		return true;
	}

	@Override
	public void layout() {
		// Nothing to layout here
	}

	/*
	 * protected void paintCachedInternal(Graphics2D g) {
	 * g.setColor(ColorScheme.BACKGROUND1);
	 * g.fillRect(0, 0, getWidth(), getHeight());
	 * g.setColor(ColorScheme.BORDER1);
	 * g.drawRect(0, 0, getWidth(), getHeight());
	 * }
	 */

	/*
	 * protected void paintUncachedInternal(Graphics2D g) {
	 * g.setColor(ColorScheme.BORDER1);
	 * BasicStroke bs = new BasicStroke(1.5f);
	 * g.setStroke(bs);
	 * Iterator<AbstractSWTVertex> it = getEdges();
	 * while(it.hasNext()) {
	 * AbstractSWTVertex target = it.next();
	 * g.drawLine(getEdgeSourceX(), getEdgeSourceY(), target.getEdgeTargetX(), target.getEdgeTargetY());
	 * }
	 * }
	 */

	private Color getBorderColor(Device device) {

		final ExecutionState executionState = this.mangementGate.getVertex().getExecutionState();
		Color returnColor = null;

		switch (executionState) {
		case RUNNING:
			returnColor = ColorScheme.getGateRunningBorderColor(device);
			break;
		case FINISHING:
			returnColor = ColorScheme.getGateFinishingBorderColor(device);
			break;
		case FINISHED:
			returnColor = ColorScheme.getGateFinishedBorderColor(device);
			break;
		case CANCELING:
		case CANCELED:
			returnColor = ColorScheme.getGateCancelBorderColor(device);
			break;
		case FAILED:
			returnColor = ColorScheme.getGateFailedBorderColor(device);
			break;
		default:
			returnColor = ColorScheme.getGateDefaultBorderColor(device);
			break;
		}

		return returnColor;
	}

	private Color getBackgroundColor(Device device) {

		final ExecutionState executionState = this.mangementGate.getVertex().getExecutionState();
		Color returnColor = null;

		switch (executionState) {
		case RUNNING:
			returnColor = ColorScheme.getGateRunningBackgroundColor(device);
			break;
		case FINISHING:
			returnColor = ColorScheme.getGateFinishingBackgroundColor(device);
			break;
		case FINISHED:
			returnColor = ColorScheme.getGateFinishedBackgroundColor(device);
			break;
		case CANCELING:
		case CANCELED:
			returnColor = ColorScheme.getGateCancelBackgroundColor(device);
			break;
		case FAILED:
			returnColor = ColorScheme.getGateFailedBackgroundColor(device);
			break;
		default:
			returnColor = ColorScheme.getGateDefaultBackgroundColor(device);
			break;
		}

		return returnColor;

	}

	@Override
	protected void paintInternal(GC gc, Device device) {

		gc.setForeground(getBorderColor(device));
		Iterator<AbstractSWTVertex> iterator = getEdges();
		while (iterator.hasNext()) {
			paintArrow(gc, iterator.next());
		}

		if (this.mangementGate.isInputGate()) {
			gc.setBackground(getBackgroundColor(device));
		} else {
			final ManagementGroupEdge groupEdge = this.mangementGate.getVertex().getGroupVertex().getForwardEdge(
				this.mangementGate.getIndex());
			final GroupEdgeVisualizationData groupEdgeVisualizationData = (GroupEdgeVisualizationData) groupEdge
				.getAttachment();
			if (groupEdgeVisualizationData.isIOBottleneck()) {
				gc.setBackground(getBackgroundColor(device));
			} else {
				gc.setBackground(getBackgroundColor(device));
			}
		}

		/*
		 * if(groupEdgeVisualizationData.isIOBottleneck()) {
		 * Color [] transition = ColorScheme.getIOBottleneckTransition(device);
		 * gc.setBackground(transition[this.colorIndex]);
		 * if(this.colorDirectionAscending) {
		 * ++this.colorIndex;
		 * if(this.colorIndex >= transition.length) {
		 * this.colorDirectionAscending = false;
		 * --this.colorIndex;
		 * }
		 * } else {
		 * --this.colorIndex;
		 * if(this.colorIndex < 0) {
		 * this.colorDirectionAscending = true;
		 * ++this.colorIndex;
		 * }
		 * }
		 * } else {
		 * this.colorIndex = 0;
		 * this.colorDirectionAscending = true;
		 * gc.setBackground(ColorScheme.getBackground1(device));
		 * //}
		 */

		gc.fillRectangle(this.rect);
		gc.drawRectangle(this.rect);
	}

	private void paintArrow(GC gc, AbstractSWTVertex target) {

		// int barb = 10;
		final int sourcex = getEdgeSourceX();
		final int sourcey = getEdgeSourceY();
		final int targetx = target.getEdgeTargetX();
		final int targety = target.getEdgeTargetY();

		gc.drawLine(sourcex, sourcey, targetx, targety);

		/*
		 * double dy = target.getEdgeTargetY() - getEdgeSourceY();
		 * double dx = target.getEdgeTargetX() - getEdgeSourceX();
		 * double theta = Math.atan2(dy, dx);
		 * double phi = 30;
		 * double x, y, rho = theta + phi;
		 * for(int j = 0; j < 2; j++){
		 * x = target.getEdgeTargetX() - barb * Math.cos(rho);
		 * y = target.getEdgeTargetY() - barb * Math.sin(rho);
		 * gc.drawLine(target.getEdgeTargetX(), target.getEdgeTargetY(), (int) Math.rint(x), (int) Math.rint(y));
		 * rho = theta - phi;
		 * }
		 */
	}

	public boolean isInputGate() {
		return this.mangementGate.isInputGate();
	}

	@Override
	public SWTToolTip constructToolTip(Shell parentShell, SWTToolTipCommandReceiver commandReceiver, int x, int y) {
		return new SWTGateToolTip(parentShell, this.mangementGate, x, y);
	}
}
