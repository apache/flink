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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.ExecutionStateChangeEvent;
import eu.stratosphere.nephele.event.job.NewJobEvent;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertexIterator;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.profiling.types.InputGateProfilingEvent;
import eu.stratosphere.nephele.profiling.types.InstanceProfilingEvent;
import eu.stratosphere.nephele.profiling.types.InstanceSummaryProfilingEvent;
import eu.stratosphere.nephele.profiling.types.OutputGateProfilingEvent;
import eu.stratosphere.nephele.profiling.types.SingleInstanceProfilingEvent;
import eu.stratosphere.nephele.profiling.types.ThreadProfilingEvent;
import eu.stratosphere.nephele.profiling.types.VertexProfilingEvent;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

public class SWTVisualizationGUI implements SelectionListener, Runnable {

	private static final Log LOG = LogFactory.getLog(SWTVisualizationGUI.class);

	private final int QUERYINTERVAL;

	private final Display display;

	private final Shell shell;

	private final Tree jobTree;

	private final boolean detectBottlenecks;

	private final ExtendedManagementProtocol jobManager;

	private final CTabFolder jobTabFolder;

	private long lastClickTime = 0;

	private Map<JobID, GraphVisualizationData> visualizableJobs = new HashMap<JobID, GraphVisualizationData>();

	/**
	 * Set to filter duplicate events received from the job manager.
	 */
	private Set<AbstractEvent> processedEvents = new HashSet<AbstractEvent>();

	public SWTVisualizationGUI(ExtendedManagementProtocol jobManager, int queryInterval) {

		this.jobManager = jobManager;
		this.QUERYINTERVAL = queryInterval;

		this.display = new Display();
		this.shell = new Shell(display);

		this.detectBottlenecks = GlobalConfiguration.getBoolean("visualization.bottleneckDetection.enable", false);

		// Set title and size
		this.shell.setText("Nephele Job Visualization");
		this.shell.setSize(1280, 1024);

		final GridLayout gridLayout = new GridLayout(1, false);
		gridLayout.marginTop = 0;
		gridLayout.marginBottom = 0;
		gridLayout.marginLeft = 0;
		gridLayout.marginRight = 0;
		gridLayout.marginWidth = 0;
		gridLayout.marginHeight = 0;

		this.shell.setLayout(gridLayout);

		new LogoCanvas(this.shell, SWT.NONE).setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		final SashForm horizontalSash = new SashForm(this.shell, SWT.HORIZONTAL);
		horizontalSash.setLayoutData(new GridData(GridData.FILL_BOTH));

		final Group jobGroup = new Group(horizontalSash, SWT.NONE);
		jobGroup.setText("Visualizable Jobs");
		jobGroup.setLayout(new FillLayout());

		this.jobTree = new Tree(jobGroup, SWT.SINGLE | SWT.BORDER);
		this.jobTree.addSelectionListener(this);

		this.jobTabFolder = new CTabFolder(horizontalSash, SWT.TOP);
		this.jobTabFolder.addSelectionListener(this);

		horizontalSash.setWeights(new int[] { 2, 8 });

		// Launch the timer that will query for events
		this.display.timerExec(QUERYINTERVAL * 1000, this);
	}

	public Shell getShell() {
		return this.shell;
	}

	public Display getDisplay() {
		return this.display;
	}

	private void createJobTab(GraphVisualizationData visualizationData) {

		final JobID jobID = visualizationData.getManagementGraph().getJobID();

		CTabItem jobTabItem = new CTabItem(this.jobTabFolder, SWT.CLOSE);
		jobTabItem.setText(visualizationData.getJobName());
		jobTabItem.setData(jobID);

		SWTJobTabItem swtTabItem = new SWTJobTabItem(this, visualizationData, this.jobTabFolder, SWT.NONE,
			this.detectBottlenecks);
		jobTabItem.setControl(swtTabItem);
	}

	@Override
	public void widgetDefaultSelected(SelectionEvent arg0) {
		// Nothing to do here
	}

	@Override
	public void widgetSelected(SelectionEvent arg0) {

		if (arg0.widget == this.jobTree) {

			final long currentTime = System.currentTimeMillis();
			if ((currentTime - this.lastClickTime) <= getDisplay().getDoubleClickTime()) {
				// Double click
				final Widget selectedWidget = arg0.widget;
				if (!(selectedWidget instanceof Tree)) {
					return;
				}

				final Tree tree = (Tree) selectedWidget;
				final TreeItem[] selectedItems = tree.getSelection();
				if (selectedItems.length != 1) {
					return;
				}

				final TreeItem selectedItem = selectedItems[0];
				final GraphVisualizationData visualizationData = (GraphVisualizationData) selectedItem.getData();
				if (visualizationData == null) {
					return;
				}

				// Check if the tab is already opened
				final int index = getJobTabIndex(visualizationData.getManagementGraph().getJobID());
				if (index >= 0) {
					this.jobTabFolder.setSelection(index);
					return;
				}

				createJobTab(visualizationData);
				this.jobTabFolder.setSelection(this.jobTabFolder.getItemCount() - 1);
			}

			// Update time stamp
			this.lastClickTime = currentTime;
		}
	}

	private int getJobTabIndex(JobID jobID) {

		for (int i = 0; i < this.jobTabFolder.getItemCount(); i++) {
			final CTabItem item = this.jobTabFolder.getItem(i);
			if (item.getData() != null && item.getData() instanceof JobID) {
				final JobID candidateID = (JobID) item.getData();
				if (candidateID.equals(jobID)) {
					return i;
				}
			}
		}

		return -1;
	}

	public void cancelTask(JobID jobId, ManagementVertexID id, String vertexName) {

		final MessageBox messageBox = new MessageBox(getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
		messageBox.setText("Confirmation");
		messageBox.setMessage("Do you really want to cancel the task " + vertexName + "(" + id.toString() + ")?");
		if (messageBox.open() != SWT.YES) {
			return;
		}

		try {
			this.jobManager.cancelTask(jobId, id);
		} catch (IOException ioe) {
			final MessageBox errorBox = new MessageBox(getShell(), SWT.ICON_ERROR);
			errorBox.setText("Error");
			errorBox.setMessage(StringUtils.stringifyException(ioe));
			errorBox.open();
		}
	}

	public void killInstance(String instanceName) {

		final MessageBox messageBox = new MessageBox(getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
		messageBox.setText("Confirmation");
		messageBox.setMessage("Do you really want to kill the instance " + instanceName + "?");
		if (messageBox.open() != SWT.YES) {
			return;
		}

		try {
			this.jobManager.killInstance(new StringRecord(instanceName));
		} catch (IOException ioe) {
			final MessageBox errorBox = new MessageBox(getShell(), SWT.ICON_ERROR);
			errorBox.setText("Error");
			errorBox.setMessage(StringUtils.stringifyException(ioe));
			errorBox.open();
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		boolean viewUpdateRequired = false;

		try {

			// Check for new jobs
			final List<NewJobEvent> newJobs = jobManager.getNewJobs();
			if (!newJobs.isEmpty()) {
				final Iterator<NewJobEvent> it = newJobs.iterator();
				while (it.hasNext()) {
					final NewJobEvent newJobEvent = it.next();
					addJob(newJobEvent.getJobID(), newJobEvent.getJobName(), newJobEvent.isProfilingAvailable());
				}
			}

			// Check for all other events
			synchronized (this.visualizableJobs) {

				final Iterator<JobID> it = this.visualizableJobs.keySet().iterator();
				while (it.hasNext()) {

					final JobID jobID = it.next();
					final List<AbstractEvent> events = this.jobManager.getEvents(jobID);

					if (!events.isEmpty()) {

						final CTabItem selectedTab = this.jobTabFolder.getSelection();
						if (selectedTab != null) {
							final JobID jobIDOfSelectedTab = (JobID) selectedTab.getData();
							if (jobID.equals(jobIDOfSelectedTab)) {
								viewUpdateRequired = true;
							}
						}

						final GraphVisualizationData graphVisualizationData = this.visualizableJobs.get(jobID);

						final Iterator<AbstractEvent> eventIt = events.iterator();
						while (eventIt.hasNext()) {

							final AbstractEvent event = eventIt.next();
							if (this.processedEvents.contains(event)) {
								continue;
							}

							dispatchEvent(event, graphVisualizationData);
						}

						// Clean up
						cleanUpOldEvents(QUERYINTERVAL * 1000);
					}
				}

			}

		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}

		if (viewUpdateRequired) {
			updateView();
		}

		this.display.timerExec(QUERYINTERVAL * 1000, this);
	}

	private void updateView() {

		final CTabItem selectedTabItem = this.jobTabFolder.getSelection();
		if (selectedTabItem == null) {
			return;
		}

		final Control control = selectedTabItem.getControl();
		if (control == null) {
			return;
		}

		if (!(control instanceof SWTJobTabItem)) {
			return;
		}

		((SWTJobTabItem) control).updateView();
	}

	private void addJob(JobID jobID, String jobName, boolean isProfilingAvailable) throws IOException {

		synchronized (this.visualizableJobs) {

			if (this.visualizableJobs.containsKey(jobID)) {
				// We already know this job
				return;
			}

			// This is a new job, request the management graph of the job and topology
			final ManagementGraph managementGraph = this.jobManager.getManagementGraph(jobID);
			final NetworkTopology networkTopology = this.jobManager.getNetworkTopology(jobID);

			// Create graph visualization object
			final GraphVisualizationData graphVisualizationData = new GraphVisualizationData(jobID, jobName,
				isProfilingAvailable, managementGraph, networkTopology);

			managementGraph.setAttachment(graphVisualizationData);
			final Iterator<ManagementVertex> it = new ManagementGraphIterator(managementGraph, true);
			while (it.hasNext()) {
				final ManagementVertex vertex = it.next();
				vertex.setAttachment(new VertexVisualizationData(isProfilingAvailable));
				for (int i = 0; i < vertex.getNumberOfOutputGates(); i++) {
					vertex.getOutputGate(i).setAttachment(new GateVisualizationData(isProfilingAvailable));
				}
				for (int i = 0; i < vertex.getNumberOfInputGates(); i++) {
					vertex.getInputGate(i).setAttachment(new GateVisualizationData(isProfilingAvailable));
				}
			}

			final Iterator<ManagementGroupVertex> it2 = new ManagementGroupVertexIterator(managementGraph, true, -1);
			while (it2.hasNext()) {
				final ManagementGroupVertex groupVertex = it2.next();
				groupVertex.setAttachment(new GroupVertexVisualizationData(groupVertex));
				for (int i = 0; i < groupVertex.getNumberOfForwardEdges(); i++) {
					final ManagementGroupEdge groupEdge = groupVertex.getForwardEdge(i);
					groupEdge.setAttachment(new GroupEdgeVisualizationData(groupEdge));
				}
			}

			final Iterator<NetworkNode> it3 = networkTopology.iterator();
			while (it3.hasNext()) {
				final NetworkNode networkNode = it3.next();
				if (networkNode.isLeafNode()) {
					networkNode.setAttachment(new InstanceVisualizationData(isProfilingAvailable));
				}
			}
			networkTopology.setAttachment(new InstanceVisualizationData(isProfilingAvailable));

			final TreeItem jobItem = new TreeItem(jobTree, SWT.NONE);
			jobItem.setText(jobName + " (" + jobID.toString() + ")");
			jobItem.setData(graphVisualizationData);

			this.visualizableJobs.put(jobID, graphVisualizationData);
		}
	}

	private void dispatchEvent(AbstractEvent event, GraphVisualizationData graphVisualizationData) {

		if (event instanceof VertexProfilingEvent) {

			final VertexProfilingEvent vertexProfilingEvent = (VertexProfilingEvent) event;
			final ManagementGraph graph = graphVisualizationData.getManagementGraph();
			final ManagementVertex vertex = graph.getVertexByID(vertexProfilingEvent.getVertexID());

			if (vertexProfilingEvent instanceof ThreadProfilingEvent) {

				final VertexVisualizationData vertexVisualizationData = (VertexVisualizationData) vertex
					.getAttachment();
				vertexVisualizationData.processThreadProfilingEvent((ThreadProfilingEvent) vertexProfilingEvent);

			} else if (vertexProfilingEvent instanceof OutputGateProfilingEvent) {

				final OutputGateProfilingEvent outputGateProfilingEvent = (OutputGateProfilingEvent) vertexProfilingEvent;
				final ManagementGate managementGate = vertex.getOutputGate(outputGateProfilingEvent.getGateIndex());
				final GateVisualizationData gateVisualizationData = (GateVisualizationData) managementGate
					.getAttachment();
				gateVisualizationData.processOutputGateProfilingEvent(outputGateProfilingEvent);

			} else if (vertexProfilingEvent instanceof InputGateProfilingEvent) {

				final InputGateProfilingEvent inputGateProfilingEvent = (InputGateProfilingEvent) vertexProfilingEvent;
				final ManagementGate managementGate = vertex.getInputGate(inputGateProfilingEvent.getGateIndex());
				final GateVisualizationData gateVisualizationData = (GateVisualizationData) managementGate
					.getAttachment();
				gateVisualizationData.processInputGateProfilingEvent(inputGateProfilingEvent);
			}
		} else if (event instanceof InstanceProfilingEvent) {

			final NetworkTopology networkTopology = graphVisualizationData.getNetworkTopology();

			if (event instanceof InstanceSummaryProfilingEvent) {

				final InstanceVisualizationData instanceVisualizationData = (InstanceVisualizationData) networkTopology
					.getAttachment();
				instanceVisualizationData.processInstanceProfilingEvent((InstanceSummaryProfilingEvent) event);

			} else {

				final SingleInstanceProfilingEvent singleInstanceProfilingEvent = (SingleInstanceProfilingEvent) event;
				final NetworkNode networkNode = networkTopology.getNodeByName(singleInstanceProfilingEvent
					.getInstanceName());
				final InstanceVisualizationData instanceVisualizationData = (InstanceVisualizationData) networkNode
					.getAttachment();
				instanceVisualizationData.processInstanceProfilingEvent(singleInstanceProfilingEvent);

			}
		} else if (event instanceof ExecutionStateChangeEvent) {

			final ExecutionStateChangeEvent executionStateChangeEvent = (ExecutionStateChangeEvent) event;
			final ManagementGraph graph = graphVisualizationData.getManagementGraph();
			final ManagementVertex vertex = graph.getVertexByID(executionStateChangeEvent.getVertexID());
			vertex.setExecutionState(executionStateChangeEvent.getNewExecutionState());

		} else {
			System.out.println("Unknown event: " + event);
		}
	}

	/**
	 * Removes entries from the set of already processed events for which
	 * it is guaranteed that no duplicates will be received anymore.
	 */
	private void cleanUpOldEvents(long sleepTime) {

		long mostRecentTimestamp = 0;

		// Find most recent time stamp
		Iterator<AbstractEvent> it = this.processedEvents.iterator();
		while (it.hasNext()) {
			final AbstractEvent event = it.next();
			if (event.getTimestamp() > mostRecentTimestamp) {
				mostRecentTimestamp = event.getTimestamp();
			}
		}

		if (mostRecentTimestamp == 0) {
			return;
		}

		/**
		 * Remove all events which older than three times
		 * the sleep time. The job manager removes events in
		 * intervals which are twice the interval of the sleep time,
		 * so there is no risk that these events will appear as duplicates
		 * again.
		 */
		it = this.processedEvents.iterator();
		while (it.hasNext()) {

			final AbstractEvent event = it.next();

			if ((event.getTimestamp() + (3 * sleepTime)) < mostRecentTimestamp) {
				it.remove();
			}
		}
	}
}
