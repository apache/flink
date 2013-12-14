/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.addons.visualization.swt;

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

import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementStage;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

public class SWTGraphCanvas extends Canvas implements PaintListener, Listener, MouseMoveListener, Runnable,
		SWTToolTipCommandReceiver {

	private final SWTGraph swtGraph;

	private final SWTJobTabItem jobTabItem;

	private final GraphVisualizationData visualizationData;

	private final boolean detectBottlenecks;


	private final String FAKE_TAIL = "Fake Tail";
	private final String ITERATION_SYNC = "Iteration Sync";

	private AbstractSWTComponent toolTipComponent = null;

	private SWTToolTip displayedToolTip = null;

	private volatile boolean runAnimation = false;
	
	

	public SWTGraphCanvas(GraphVisualizationData visualizationData, SWTJobTabItem jobTabItem, Composite parent,
			int style, boolean detectBottlenecks) {
		super(parent, style);

		this.visualizationData = visualizationData;
		this.swtGraph = createSWTGraph(visualizationData);
		this.jobTabItem = jobTabItem;
		this.detectBottlenecks = detectBottlenecks;

		// Important: SWT.RESIZE does not equal SWT.Resize
		addMouseMoveListener(this);
		addListener(SWT.Resize, this);
		addPaintListener(this);
	}

	@Override
	public void paintControl(PaintEvent arg0) {

		// Paint the graph
		this.swtGraph.paint(arg0.gc, getDisplay());
	}

	private SWTGraph createSWTGraph(GraphVisualizationData visualizationData) {

		final ManagementGraph managementGraph = visualizationData.getManagementGraph();
		final SWTGraph swtGraph = new SWTGraph();

		Map<ManagementStage, SWTStage> stageMap = addManagementStages(swtGraph, managementGraph);
		Map<ManagementGroupVertex, SWTGroupVertex> groupMap = addGroupVertices(stageMap);
		addExecutionVertices(groupMap, managementGraph, visualizationData);

		return swtGraph;
	}

	private Map<ManagementStage, SWTStage> addManagementStages(SWTGraph swtGraph, ManagementGraph managementGraph) {

		Map<ManagementStage, SWTStage> stageMap = new HashMap<ManagementStage, SWTStage>();
		SWTStage previousVisualStage = null;

		for (int i = 0; i < managementGraph.getNumberOfStages(); i++) {

			ManagementStage executionStage = managementGraph.getStage(i);
			SWTStage visualStage = new SWTStage(swtGraph);

			stageMap.put(executionStage, visualStage);

			if (previousVisualStage != null) {
				previousVisualStage.connectTo(visualStage);
			}

			previousVisualStage = visualStage;
		}

		return stageMap;
	}

	private Map<ManagementGroupVertex, SWTGroupVertex> addGroupVertices(Map<ManagementStage, SWTStage> stageMap) {

		Map<ManagementGroupVertex, SWTGroupVertex> groupMap = new HashMap<ManagementGroupVertex, SWTGroupVertex>();

		// First, create all vertices
		Iterator<Map.Entry<ManagementStage, SWTStage>> iterator = stageMap.entrySet().iterator();
		while (iterator.hasNext()) {

			final Map.Entry<ManagementStage, SWTStage> entry = iterator.next();
			final ManagementStage managementStage = entry.getKey();
			final SWTStage parent = entry.getValue();

			for (int i = 0; i < managementStage.getNumberOfGroupVertices(); i++) {

				final ManagementGroupVertex groupVertex = managementStage.getGroupVertex(i);

				// hide unwanted elements of iteration pact plans				
				if (!(groupVertex.getName().equals(this.FAKE_TAIL)|| groupVertex.getName().contains(this.ITERATION_SYNC)) ){
					final SWTGroupVertex visualGroupVertex = new SWTGroupVertex(parent, groupVertex);				
					groupMap.put(groupVertex, visualGroupVertex);
				}
			}
		}

		// Second, make sure all edges are created and connected properly
		iterator = stageMap.entrySet().iterator();
		while (iterator.hasNext()) {

			final Map.Entry<ManagementStage, SWTStage> entry = iterator.next();
			final ManagementStage executionStage = entry.getKey();
			for (int i = 0; i < executionStage.getNumberOfGroupVertices(); i++) {

				final ManagementGroupVertex sourceVertex = executionStage.getGroupVertex(i);
				final SWTGroupVertex sourceGroupVertex = groupMap.get(sourceVertex);

				for (int j = 0; j < sourceVertex.getNumberOfForwardEdges(); j++) {

					final ManagementGroupEdge edge = sourceVertex.getForwardEdge(j);
					final ManagementGroupVertex targetVertex = edge.getTarget();
					final SWTGroupVertex targetGroupVertex = groupMap.get(targetVertex);
					sourceGroupVertex.connectTo(targetGroupVertex);
				}

			}
		}

		return groupMap;
	}

	private void addExecutionVertices(Map<ManagementGroupVertex, SWTGroupVertex> groupMap,
			ManagementGraph managementGraph, GraphVisualizationData visualizationData) {

		ManagementGraphIterator iterator = new ManagementGraphIterator(managementGraph, true);
		final Map<ManagementVertex, SWTVertex> vertexMap = new HashMap<ManagementVertex, SWTVertex>();
		final Map<ManagementGate, SWTGate> gateMap = new HashMap<ManagementGate, SWTGate>();

		while (iterator.hasNext()) {

			final ManagementVertex mv = iterator.next();
			final SWTGroupVertex parent = groupMap.get(mv.getGroupVertex());

			// draw connections only for elements that are not hidden
			if ( !(mv.getName().equals(this.FAKE_TAIL) || mv.getName().contains(this.ITERATION_SYNC))){

				final SWTVertex visualVertex = new SWTVertex(parent, mv);
				vertexMap.put(mv, visualVertex);
	
				for (int gateIndex = 0; gateIndex < mv.getNumberOfOutputGates(); gateIndex++) {
					final ManagementGate outputGate = mv.getOutputGate(gateIndex);
					final SWTGate visualGate = new SWTGate(visualVertex, outputGate);
					gateMap.put(outputGate, visualGate);
				}

				for (int gateIndex = 0; gateIndex < mv.getNumberOfInputGates(); gateIndex++) {
					final ManagementGate inputGate = mv.getInputGate(gateIndex);
					final SWTGate visualGate = new SWTGate(visualVertex, inputGate);
					gateMap.put(inputGate, visualGate);
				}
			}
		}

		iterator = new ManagementGraphIterator(managementGraph, true);

		// Setup connections
		while (iterator.hasNext()) {

			final ManagementVertex source = iterator.next();
			final SWTVertex visualSource = vertexMap.get(source);

			for (int i = 0; i < source.getNumberOfOutputGates(); i++) {

				final ManagementGate sourceGate = source.getOutputGate(i);
				final SWTGate visualSourceGate = gateMap.get(sourceGate);

				for (int j = 0; j < sourceGate.getNumberOfForwardEdges(); j++) {

					final ManagementEdge forwardEdge = sourceGate.getForwardEdge(j);
					final SWTVertex visualTarget = vertexMap.get(forwardEdge.getTarget().getVertex());
					final SWTGate visualTargetGate = gateMap.get(forwardEdge.getTarget());

					visualSource.connectTo(visualTarget);
					visualSourceGate.connectTo(visualTargetGate);
				}
			}
		}
	}

	@Override
	public void handleEvent(Event arg0) {

		if (this.swtGraph != null) {

			final Rectangle rect = getClientArea();
			this.swtGraph.setX(rect.x);
			this.swtGraph.setY(rect.y);
			this.swtGraph.setWidth(rect.width);
			this.swtGraph.setHeight(rect.height);
			this.swtGraph.layout();
		}
	}

	@Override
	public void mouseMove(MouseEvent arg0) {

		final AbstractSWTComponent selectedComponent = this.swtGraph.getSelectedComponent(arg0.x, arg0.y);
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

	public void updateView() {

		// Check for bottlenecks
		if (this.detectBottlenecks) {
			this.visualizationData.detectBottlenecks();
		}

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
	public void run() {

		if (!isDisposed()) {

			redraw();

			if (runAnimation) {
				getDisplay().timerExec(50, this);
			}
		}
	}

	@Override
	public void switchToInstance(String instanceName) {

		// Close the tooltip
		if (this.displayedToolTip != null) {
			if (!this.displayedToolTip.isDisposed()) {
				this.displayedToolTip.dispose();
			}

			this.displayedToolTip = null;
			this.toolTipComponent = null;
		}

		// Delegate call to job tab item
		this.jobTabItem.switchToInstance(instanceName);
	}

	public void startAnimation() {

		if (runAnimation) {
			return;
		}

		if (!getDisplay().isDisposed()) {
			getDisplay().timerExec(50, this);
			runAnimation = true;
		}
	}

	public void stopAnimation() {
		runAnimation = false;
	}

	@Override
	public void killTask(final ManagementVertexID id, final String taskName) {

		// Delegate call to the job tab item
		this.jobTabItem.killTask(id, taskName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killInstance(final String instanceName) {

		// Nothing to do here
	}
}
