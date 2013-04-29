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
import java.util.Collections;
import java.util.Comparator;
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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.CheckpointStateChangeEvent;
import eu.stratosphere.nephele.event.job.ExecutionStateChangeEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.event.job.VertexAssignmentEvent;
import eu.stratosphere.nephele.event.job.VertexEvent;
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

	private static final String JAVA_DOC_URL = "http://www.stratosphere.eu/";

	private final int QUERYINTERVAL;

	private final Display display;

	private final Menu menuBar;

	private final Shell shell;

	private final Tree jobTree;

	private final boolean detectBottlenecks;

	private volatile boolean applyFailurePatterns = true;

	private final ExtendedManagementProtocol jobManager;

	private final CTabFolder jobTabFolder;

	private long lastClickTime = 0;

	private Map<JobID, GraphVisualizationData> recentJobs = new HashMap<JobID, GraphVisualizationData>();

	private final SWTFailurePatternsManager failurePatternsManager;

	/**
	 * The sequence number of the last processed event received from the job manager.
	 */
	private long lastProcessedEventSequenceNumber = -1;

	public SWTVisualizationGUI(ExtendedManagementProtocol jobManager, int queryInterval) {

		this.jobManager = jobManager;
		this.QUERYINTERVAL = queryInterval;

		this.display = new Display();
		this.shell = new Shell(this.display);

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
		jobGroup.setText("Recent Jobs");
		jobGroup.setLayout(new FillLayout());

		this.jobTree = new Tree(jobGroup, SWT.SINGLE | SWT.BORDER);
		this.jobTree.addSelectionListener(this);

		// Disable native tooltip implementation
		this.jobTree.setToolTipText("");

		// Implementation of the extended tree tooltips
		final Listener toolTipListener = new Listener() {

			private SWTJobToolTip jobToolTip = null;

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void handleEvent(final Event event) {

				switch (event.type) {
				case SWT.Dispose:
				case SWT.KeyDown:
				case SWT.MouseMove:
					if (this.jobToolTip != null) {
						this.jobToolTip.dispose();
						this.jobToolTip = null;
					}
					break;
				case SWT.MouseHover:
					final TreeItem ti = jobTree.getItem(new Point(event.x, event.y));
					if (ti == null) {
						break;
					}
					if (this.jobToolTip != null && !this.jobToolTip.isDisposed()) {
						this.jobToolTip.dispose();
					}

					final Point pt = jobTree.toDisplay(event.x, event.y);

					final GraphVisualizationData gvi = (GraphVisualizationData) ti.getData();
					if (gvi == null) {
						break;
					}

					final String jobName = gvi.getJobName();
					final JobID jobID = gvi.getJobID();
					final long submissionTimestamp = gvi.getSubmissionTimestamp();

					this.jobToolTip = new SWTJobToolTip(shell, jobName, jobID, submissionTimestamp, pt.x, pt.y);
					break;
				}

			}

		};

		// Register tooltip listener
		this.jobTree.addListener(SWT.Dispose, toolTipListener);
		this.jobTree.addListener(SWT.KeyDown, toolTipListener);
		this.jobTree.addListener(SWT.MouseMove, toolTipListener);
		this.jobTree.addListener(SWT.MouseHover, toolTipListener);

		this.jobTabFolder = new CTabFolder(horizontalSash, SWT.TOP);
		this.jobTabFolder.addSelectionListener(this);

		horizontalSash.setWeights(new int[] { 2, 8 });

		// Construct the menu
		this.menuBar = new Menu(this.shell, SWT.BAR);

		final MenuItem fileMenuItem = new MenuItem(this.menuBar, SWT.CASCADE);
		fileMenuItem.setText("&File");

		final Menu fileMenu = new Menu(this.shell, SWT.DROP_DOWN);
		fileMenuItem.setMenu(fileMenu);

		final MenuItem fileExitItem = new MenuItem(fileMenu, SWT.PUSH);
		fileExitItem.setText("E&xit");
		fileExitItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				shell.close();
				display.dispose();
			}
		});

		final MenuItem jobMenuItem = new MenuItem(this.menuBar, SWT.CASCADE);
		jobMenuItem.setText("&Job");

		final Menu jobMenu = new Menu(this.shell, SWT.DROP_DOWN);
		jobMenuItem.setMenu(jobMenu);

		final MenuItem cancelJobItem = new MenuItem(jobMenu, SWT.PUSH);
		cancelJobItem.setText("&Cancel job");
		cancelJobItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				cancelJob();
				shell.setMenuBar(null);
			}
		});

		final MenuItem debuggingMenuItem = new MenuItem(this.menuBar, SWT.CASCADE);
		debuggingMenuItem.setText("&Debugging");

		final Menu debuggingMenu = new Menu(this.shell, SWT.DROP_DOWN);
		debuggingMenuItem.setMenu(debuggingMenu);

		final MenuItem debuggingLBUItem = new MenuItem(debuggingMenu, SWT.PUSH);
		debuggingLBUItem.setText("&Log buffer utilization");
		debuggingLBUItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				logBufferUtilization();
				shell.setMenuBar(null);
			}
		});

		// Insert a separator before the last item in the help menu
		new MenuItem(debuggingMenu, SWT.SEPARATOR);

		final MenuItem debuggingAFPItem = new MenuItem(debuggingMenu, SWT.CHECK);
		debuggingAFPItem.setText("&Apply failure patterns");
		debuggingAFPItem.setSelection(this.applyFailurePatterns);
		debuggingAFPItem.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent arg0) {
				applyFailurePatterns = debuggingAFPItem.getSelection();
				shell.setMenuBar(null);
			}
		});

		final MenuItem debuggingMFPItem = new MenuItem(debuggingMenu, SWT.PUSH);
		debuggingMFPItem.setText("&Manage failure patterns...");
		debuggingMFPItem.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent arg0) {
				manageFailurePatterns();
				shell.setMenuBar(null);
			}
		});

		final MenuItem helpMenuItem = new MenuItem(this.menuBar, SWT.CASCADE);
		helpMenuItem.setText("&Help");

		final Menu helpMenu = new Menu(this.shell, SWT.DROP_DOWN);
		helpMenuItem.setMenu(helpMenu);

		final MenuItem helpJavaDocItem = new MenuItem(helpMenu, SWT.PUSH);
		helpJavaDocItem.setText("&View JavaDoc...");
		helpJavaDocItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				viewJavaDoc();
				shell.setMenuBar(null);
			}
		});

		// Insert a separator before the last item in the help menu
		new MenuItem(helpMenu, SWT.SEPARATOR);

		final MenuItem helpAboutItem = new MenuItem(helpMenu, SWT.PUSH);
		helpAboutItem.setText("&About...");
		helpAboutItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				showAboutDialog();
				shell.setMenuBar(null);
			}
		});

		// Make sure we display the menu whenever the user presses ALT
		this.display.addFilter(SWT.KeyDown, new Listener() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void handleEvent(final Event arg0) {

				if (arg0.keyCode == SWT.ALT) {

					if (shell.getMenuBar() == null) {
						shell.setMenuBar(menuBar);
					} else {
						shell.setMenuBar(null);
					}
				}

			}
		});

		// Make sure the menu disappears whenever the user clicks on something other than the menu
		this.display.addFilter(SWT.MouseDown, new Listener() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void handleEvent(final Event arg0) {

				if (shell.getMenuBar() != null) {
					shell.setMenuBar(null);
				}
			}
		});

		// Create failure patterns manager
		this.failurePatternsManager = new SWTFailurePatternsManager(this.shell.getDisplay(), jobManager);

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void widgetDefaultSelected(SelectionEvent arg0) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
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

	public void killTask(JobID jobId, ManagementVertexID id, String vertexName) {

		final MessageBox messageBox = new MessageBox(getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
		messageBox.setText("Confirmation");
		messageBox.setMessage("Do you really want to cancel the task " + vertexName + "(" + id.toString() + ")?");
		if (messageBox.open() != SWT.YES) {
			return;
		}

		try {
			this.jobManager.killTask(jobId, id);
		} catch (IOException ioe) {
			final MessageBox errorBox = new MessageBox(getShell(), SWT.ICON_ERROR);
			errorBox.setText("Error");
			errorBox.setMessage(StringUtils.stringifyException(ioe));
			errorBox.open();
		}
	}

	public void killInstance(final String instanceName) {

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
			final List<RecentJobEvent> newJobs = this.jobManager.getRecentJobs();

			// Sort jobs according to submission time stamps
			Collections.sort(newJobs, new Comparator<RecentJobEvent>() {

				@Override
				public int compare(final RecentJobEvent o1, final RecentJobEvent o2) {

					return (int) (o1.getSubmissionTimestamp() - o2.getSubmissionTimestamp());
				}
			});

			if (!newJobs.isEmpty()) {
				final Iterator<RecentJobEvent> it = newJobs.iterator();
				while (it.hasNext()) {
					final RecentJobEvent newJobEvent = it.next();
					addJob(newJobEvent.getJobID(), newJobEvent.getJobName(), newJobEvent.isProfilingAvailable(),
						newJobEvent.getSubmissionTimestamp(), newJobEvent.getTimestamp());
				}
			}

			// Check for all other events
			synchronized (this.recentJobs) {

				final Iterator<JobID> it = this.recentJobs.keySet().iterator();
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

						final GraphVisualizationData graphVisualizationData = this.recentJobs.get(jobID);

						final Iterator<AbstractEvent> eventIt = events.iterator();
						while (eventIt.hasNext()) {

							final AbstractEvent event = eventIt.next();

							// Did we already process this event?
							if (this.lastProcessedEventSequenceNumber >= event.getSequenceNumber()) {
								continue;
							}

							dispatchEvent(event, graphVisualizationData);

							this.lastProcessedEventSequenceNumber = event.getSequenceNumber();
						}

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

	private void addJob(JobID jobID, String jobName, boolean isProfilingAvailable, final long submissionTimestamp,
			final long referenceTime) throws IOException {

		synchronized (this.recentJobs) {

			if (this.recentJobs.containsKey(jobID)) {
				// We already know this job
				return;
			}

			// This is a new job, request the management graph of the job and topology
			final ManagementGraph managementGraph = this.jobManager.getManagementGraph(jobID);
			final NetworkTopology networkTopology = this.jobManager.getNetworkTopology(jobID);

			// Create graph visualization object
			final GraphVisualizationData graphVisualizationData = new GraphVisualizationData(jobID, jobName,
				isProfilingAvailable, submissionTimestamp, managementGraph, networkTopology);

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

			// Find a matching failure pattern and start it
			this.failurePatternsManager.startFailurePattern(jobName, managementGraph, referenceTime);

			this.recentJobs.put(jobID, graphVisualizationData);
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

		} else if (event instanceof VertexAssignmentEvent) {

			final VertexAssignmentEvent vertexAssignmentEvent = (VertexAssignmentEvent) event;
			final ManagementGraph graph = graphVisualizationData.getManagementGraph();
			final ManagementVertex vertex = graph.getVertexByID(vertexAssignmentEvent.getVertexID());
			vertex.setInstanceName(vertexAssignmentEvent.getInstanceName());
			vertex.setInstanceType(vertexAssignmentEvent.getInstanceType());
		} else if (event instanceof CheckpointStateChangeEvent) {

			final CheckpointStateChangeEvent checkpointStateChangeEvent = (CheckpointStateChangeEvent) event;
			final ManagementGraph graph = graphVisualizationData.getManagementGraph();
			final ManagementVertex vertex = graph.getVertexByID(checkpointStateChangeEvent.getVertexID());
			vertex.setCheckpointState(checkpointStateChangeEvent.getNewCheckpointState());

		} else if (event instanceof JobEvent) {
			// Ignore this type of event
		} else if (event instanceof VertexEvent) {
			// Ignore this type of event
		} else {
			System.out.println("Unknown event: " + event);
		}
	}

	private void manageFailurePatterns() {

		final Set<String> jobSuggestions = new HashSet<String>();
		final Set<String> nameSuggestions = new HashSet<String>();

		final Iterator<GraphVisualizationData> it = this.recentJobs.values().iterator();
		while (it.hasNext()) {

			final GraphVisualizationData gvd = it.next();

			jobSuggestions.add(gvd.getJobName());

			final ManagementGraphIterator mgi = new ManagementGraphIterator(gvd.getManagementGraph(), true);
			while (mgi.hasNext()) {

				final ManagementVertex vertex = mgi.next();
				final String vertexName = SWTFailurePatternsManager.getSuggestedName(vertex);
				nameSuggestions.add(vertexName);
				if (vertex.getInstanceName() != null) {
					nameSuggestions.add(vertex.getInstanceName());
				}
			}
		}

		this.failurePatternsManager.openEditor(this.shell, jobSuggestions, nameSuggestions);
	}

	private void logBufferUtilization() {

		if (this.jobTree.getItemCount() == 0) {
			final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_ERROR);
			msgBox.setText("No job available");
			msgBox.setMessage("Unable to log buffer utilization because no job is available.");
			msgBox.open();
			return;
		}

		final TreeItem[] selectedItems = this.jobTree.getSelection();
		if (selectedItems.length == 0) {
			final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_INFORMATION);
			msgBox.setText("No job selected");
			msgBox
				.setMessage("Please select at least one job for which the current buffer utilization shall be logged.");
			msgBox.open();
			return;
		}

		for (int i = 0; i < selectedItems.length; i++) {

			final TreeItem selectedItem = selectedItems[i];
			final GraphVisualizationData visualizationData = (GraphVisualizationData) selectedItem.getData();
			if (visualizationData == null) {
				continue;
			}

			try {
				this.jobManager.logBufferUtilization(visualizationData.getJobID());
			} catch (IOException ioe) {
				final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_ERROR);
				msgBox.setText("Logging failed for job " + visualizationData.getJobID());
				msgBox.setMessage("Logging of buffer utilization failed for job " + visualizationData.getJobID()
					+ ":\r\n\r\n" + ioe.getMessage());
			}
		}

		final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_INFORMATION);
		msgBox.setText("Logging succesfull");
		msgBox
			.setMessage("The buffer utilization of the selected jobs have been successfully written to the instances' log files.");
		msgBox.open();
	}

	private void showAboutDialog() {

		final SWTAboutDialog aboutDialog = new SWTAboutDialog(this.shell);
		aboutDialog.open();
	}

	private void viewJavaDoc() {

		org.eclipse.swt.program.Program.launch(JAVA_DOC_URL);
	}

	private void cancelJob() {

		if (this.jobTree.getItemCount() == 0) {
			final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_ERROR);
			msgBox.setText("No job available");
			msgBox.setMessage("No job to cancel.");
			msgBox.open();
			return;
		}

		final TreeItem[] selectedItems = this.jobTree.getSelection();
		if (selectedItems.length == 0) {
			final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_INFORMATION);
			msgBox.setText("No job selected");
			msgBox.setMessage("Please select at least one job to cancel.");
			msgBox.open();
			return;
		}

		for (int i = 0; i < selectedItems.length; i++) {

			final TreeItem selectedItem = selectedItems[i];
			final GraphVisualizationData visualizationData = (GraphVisualizationData) selectedItem.getData();
			if (visualizationData == null) {
				continue;
			}

			try {
				final JobCancelResult cjr = this.jobManager.cancelJob(visualizationData.getJobID());

				if (cjr.getReturnCode() == AbstractJobResult.ReturnCode.ERROR) {
					final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_ERROR);
					msgBox.setText("Canceling job " + visualizationData.getJobID() + " failed");
					msgBox.setMessage("Canceling job " + visualizationData.getJobID()
						+ " failed:\r\n\r\n" + cjr.getDescription());
				}

			} catch (IOException ioe) {
				final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_ERROR);
				msgBox.setText("Canceling job " + visualizationData.getJobID() + " failed");
				msgBox.setMessage("Canceling job " + visualizationData.getJobID()
					+ " failed:\r\n\r\n" + ioe.getMessage());
			}
		}

		final MessageBox msgBox = new MessageBox(this.shell, SWT.OK | SWT.ICON_INFORMATION);
		msgBox.setText("Job(s) succesfully canceled");
		msgBox.setMessage("The selected jobs have been successfully canceled.");
		msgBox.open();

	}
}
