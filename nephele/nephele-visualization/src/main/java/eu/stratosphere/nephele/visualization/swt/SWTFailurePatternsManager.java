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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.MenuEvent;
import org.eclipse.swt.events.MenuListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import eu.stratosphere.nephele.jobgraph.JobID;

public final class SWTFailurePatternsManager extends SelectionAdapter {

	private static final Log LOG = LogFactory.getLog(SWTFailurePatternsManager.class);

	private static final int WIDTH = 800;

	private static final int HEIGHT = 400;

	private static final int ICON_COLUMN_WIDTH = 20;

	private static final int TEXT_COLUMN_WIDTH = 200;

	private final Shell shell;

	private final Tree jobTree;

	private final Table failureEventTable;

	private final Map<String, JobFailurePattern> failurePatterns = new HashMap<String, JobFailurePattern>();

	private JobFailurePattern selectedFailurePattern = null;

	SWTFailurePatternsManager(final Shell parent) {

		// Set size
		this.shell = new Shell(parent);
		this.shell.setSize(WIDTH, HEIGHT);
		this.shell.setText("Manage Failure Patterns");
		GridLayout gl = new GridLayout(1, false);
		gl.horizontalSpacing = 0;
		gl.verticalSpacing = 0;
		gl.marginRight = 0;
		gl.marginLeft = 0;
		gl.marginBottom = 0;
		gl.marginTop = 0;
		gl.marginHeight = 0;
		gl.marginWidth = 0;
		this.shell.setLayout(gl);

		final Composite mainComposite = new Composite(this.shell, SWT.NONE);
		mainComposite.setLayout(new GridLayout(1, false));
		GridData gridData = new GridData();
		gridData.horizontalAlignment = GridData.FILL;
		gridData.verticalAlignment = GridData.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace = true;
		mainComposite.setLayoutData(gridData);

		final SashForm horizontalSash = new SashForm(mainComposite, SWT.HORIZONTAL);
		horizontalSash.setLayoutData(new GridData(GridData.FILL_BOTH));

		final Group jobGroup = new Group(horizontalSash, SWT.NONE);
		jobGroup.setText("Job Failure Patterns");
		jobGroup.setLayout(new FillLayout());

		this.jobTree = new Tree(jobGroup, SWT.SINGLE | SWT.BORDER);
		this.jobTree.addSelectionListener(this);
		this.jobTree.setMenu(createTreeContextMenu());

		this.failureEventTable = createFailureEventTable(horizontalSash);
		horizontalSash.setWeights(new int[] { 2, 8 });

		final Composite buttonComposite = new Composite(this.shell, SWT.NONE);
		buttonComposite.setLayout(new GridLayout(2, false));
		gridData = new GridData();
		gridData.horizontalAlignment = GridData.FILL;
		buttonComposite.setLayoutData(gridData);

		final Label fillLabel = new Label(buttonComposite, SWT.NONE);
		gridData = new GridData();
		gridData.horizontalAlignment = GridData.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace = false;
		fillLabel.setLayoutData(gridData);

		final Button closeButton = new Button(buttonComposite, SWT.PUSH);
		closeButton.setText("Close");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.RIGHT;
		closeButton.setLayoutData(gridData);

		// Initialize the tables
		displayFailurePattern(null);
	}

	private Table createFailureEventTable(final Composite parent) {

		final Table table = new Table(parent, SWT.BORDER | SWT.MULTI);
		table.setHeaderVisible(true);
		table.setLinesVisible(true);

		final TableColumn iconColumn = new TableColumn(table, SWT.NONE);
		final TableColumn nameColumn = new TableColumn(table, SWT.NONE);
		nameColumn.setText("Name");
		final TableColumn intervalColumn = new TableColumn(table, SWT.NONE);
		intervalColumn.setText("Interval");

		for (int i = 0; i < table.getColumnCount(); ++i) {
			if (i == 0) {
				table.getColumn(i).setWidth(ICON_COLUMN_WIDTH);
			} else {
				table.getColumn(i).setWidth(TEXT_COLUMN_WIDTH);
			}
		}

		// Implement listener to add and update events
		table.addMouseListener(new MouseAdapter() {

			@Override
			public void mouseDoubleClick(final MouseEvent arg0) {

				final TableItem ti = table.getItem(new Point(arg0.x, arg0.y));

				if (selectedFailurePattern == null) {
					return;
				}

				final List<String> suggestions = new ArrayList<String>(); // TODO: Compute proper same suggestions here

				AbstractFailureEvent oldEvent = null;
				if (ti != null) {
					oldEvent = (AbstractFailureEvent) ti.getData();
				}

				final SWTFailureEventEditor editor = new SWTFailureEventEditor(shell, suggestions, oldEvent);

				final AbstractFailureEvent newEvent = editor.showDialog();
				if (newEvent == null) {
					return;
				}

				if (oldEvent != null) {
					selectedFailurePattern.removeEvent(oldEvent);
				}
				selectedFailurePattern.addEvent(newEvent);

				updateTableItem(ti, newEvent);
			}
		});

		// Implement sorting of columns
		final Listener sortListener = new Listener() {

			@Override
			public void handleEvent(final Event arg0) {

				final TableColumn sortColumn = failureEventTable.getSortColumn();
				final TableColumn currentColumn = (TableColumn) arg0.widget;
				int dir = failureEventTable.getSortDirection();
				if (sortColumn == currentColumn) {
					dir = (dir == SWT.UP) ? SWT.DOWN : SWT.UP;
				} else {
					table.setSortColumn(currentColumn);
					dir = SWT.UP;
				}

				final int direction = dir;
				final AbstractFailureEvent[] failureEvents = new AbstractFailureEvent[table.getItemCount()];
				for (int i = 0; i < table.getItemCount(); ++i) {
					failureEvents[i] = (AbstractFailureEvent) failureEventTable.getItem(i).getData();
				}
				Arrays.sort(failureEvents, new Comparator<AbstractFailureEvent>() {

					@Override
					public int compare(final AbstractFailureEvent o1, AbstractFailureEvent o2) {

						if (o1 == null) {
							return -1;
						}

						if (o2 == null) {
							return 1;
						}

						if (currentColumn == iconColumn) {

							final int v1 = (o1 instanceof VertexFailureEvent) ? 0 : 1;
							final int v2 = (o2 instanceof VertexFailureEvent) ? 0 : 1;
							return (direction == SWT.UP) ? (v1 - v2) : (v2 - v1);

						} else if (currentColumn == nameColumn) {

							if (direction == SWT.UP) {
								return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
							} else {
								return String.CASE_INSENSITIVE_ORDER.compare(o2.getName(), o1.getName());
							}

						} else {

							if (direction == SWT.UP) {
								return (o1.getInterval() - o2.getInterval());
							} else {
								return (o2.getInterval() - o1.getInterval());
							}
						}
					}
				});

				failureEventTable.removeAll();
				for (int i = 0; i < failureEvents.length; ++i) {
					updateTableItem(null, failureEvents[i]);
				}

				failureEventTable.setSortColumn(currentColumn);
				failureEventTable.setSortDirection(direction);
			}
		};

		iconColumn.addListener(SWT.Selection, sortListener);
		nameColumn.addListener(SWT.Selection, sortListener);
		intervalColumn.addListener(SWT.Selection, sortListener);

		return table;
	}

	private void updateTableItem(TableItem ti, final AbstractFailureEvent event) {

		boolean newItemCreated = false;

		if (ti == null) {

			final int index = (failureEventTable.getItemCount() == 0) ? 0 : (this.failureEventTable.getItemCount() - 1);

			ti = new TableItem(this.failureEventTable, SWT.NONE, index);
			newItemCreated = true;
		}

		if (event != null) {
			if (event instanceof VertexFailureEvent) {
				ti.setText(0, "T");
			} else {
				ti.setText(0, "I");
			}

			ti.setText(1, event.getName());
			ti.setText(2, Integer.toString(event.getInterval()));
		}

		// Add new blank item if the old one has been used to create the new event
		if (ti.getData() == null && !newItemCreated) {
			new TableItem(this.failureEventTable, SWT.NONE);
		}

		ti.setData(event);
	}

	public void open() {

		this.shell.open();

	}

	private Menu createTreeContextMenu() {

		final Menu treeContextMenu = new Menu(this.shell);
		final MenuItem createItem = new MenuItem(treeContextMenu, SWT.PUSH);
		createItem.setText("Create...");
		createItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				createNewFailurePattern();
			}
		});
		new MenuItem(treeContextMenu, SWT.SEPARATOR);
		final MenuItem deleteItem = new MenuItem(treeContextMenu, SWT.PUSH);
		deleteItem.setText("Delete...");
		deleteItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				deleteFailurePattern();
			}
		});
		new MenuItem(treeContextMenu, SWT.SEPARATOR);
		final MenuItem saveItem = new MenuItem(treeContextMenu, SWT.PUSH);
		saveItem.setText("Save...");
		saveItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				saveFailurePattern();
			}
		});
		final MenuItem loadItem = new MenuItem(treeContextMenu, SWT.PUSH);
		loadItem.setText("Load...");
		loadItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				loadFailurePattern();
			}
		});

		treeContextMenu.addMenuListener(new MenuListener() {

			@Override
			public void menuShown(final MenuEvent arg0) {

				if (jobTree.getSelection().length == 0) {
					createItem.setEnabled(true);
					deleteItem.setEnabled(false);
					saveItem.setEnabled(false);
					loadItem.setEnabled(true);
				} else {
					createItem.setEnabled(false);
					deleteItem.setEnabled(true);
					saveItem.setEnabled(true);
					loadItem.setEnabled(false);
				}
			}

			@Override
			public void menuHidden(final MenuEvent arg0) {
				// TODO Auto-generated method stub

			}
		});

		return treeContextMenu;
	}

	private void createNewFailurePattern() {

		// TODO: Provide proper list of name suggestions
		final List<String> suggestions = new ArrayList<String>();

		final SWTNewFailurePatternDialog dialog = new SWTNewFailurePatternDialog(this.shell, suggestions);

		final String patternName = dialog.showDialog();
		if (patternName == null) {
			return;
		}

		final JobFailurePattern jobFailurePattern = new JobFailurePattern(patternName);

		addFailurePatternToTree(jobFailurePattern);
		displayFailurePattern(jobFailurePattern);
	}

	private void deleteFailurePattern() {
		// TODO: Implement me
	}

	private void saveFailurePattern() {
		// TODO: Implement me
	}

	private void loadFailurePattern() {

		final FileDialog fileDialog = new FileDialog(this.shell, SWT.OPEN);
		fileDialog.setText("Load Failure Pattern");
		final String[] filterExts = { "*.xml", "*.*" };
		fileDialog.setFilterExtensions(filterExts);

		final String selectedFile = fileDialog.open();
		if (selectedFile == null) {
			return;
		}

		final JobFailurePattern failurePattern = loadFailurePatternFromFile(selectedFile);

		addFailurePatternToTree(failurePattern);
		displayFailurePattern(failurePattern);
	}

	private void addFailurePatternToTree(final JobFailurePattern failurePattern) {

		final TreeItem jobFailureItem = new TreeItem(this.jobTree, SWT.NONE);
		jobFailureItem.setText(failurePattern.getName());
		jobFailureItem.setData(failurePattern);
	}

	private JobFailurePattern loadFailurePatternFromFile(final String filename) {

		final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// Ignore comments in the XML file
		docBuilderFactory.setIgnoringComments(true);
		docBuilderFactory.setNamespaceAware(true);

		JobFailurePattern jobFailurePattern = null;
		InputStream inputStream = null;

		try {

			inputStream = new FileInputStream(filename);

			final DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			Document doc = null;
			Element root = null;

			doc = builder.parse(inputStream);

			if (doc == null) {
				throw new Exception("Document is null");
			}

			root = doc.getDocumentElement();
			if (root == null) {
				throw new Exception("Root element is null");
			}

			if (!"pattern".equals(root.getNodeName())) {
				throw new Exception("Encountered unknown element " + root.getNodeName());
			}

			final NodeList patternChildren = root.getChildNodes();
			for (int i = 0; i < patternChildren.getLength(); ++i) {

				final Node patternChild = patternChildren.item(i);

				if (patternChild instanceof org.w3c.dom.Text) {
					continue;
				}

				if (patternChild instanceof Element) {

					final Element patternElement = (Element) patternChild;
					if ("name".equals(patternElement.getNodeName())) {
						final String name = extractValueFromElement(patternElement);
						if (jobFailurePattern != null) {
							throw new Exception("Element name detected more than once in the file");
						}

						jobFailurePattern = new JobFailurePattern(name);
						continue;
					}

					if ("failures".equals(patternElement.getNodeName())) {

						if (jobFailurePattern == null) {
							throw new Exception("Expected pattern name to be stored before the failure events");
						}

						final NodeList failuresChildren = patternElement.getChildNodes();
						for (int j = 0; j < failuresChildren.getLength(); ++j) {

							final Node failuresChild = failuresChildren.item(j);

							if (failuresChild instanceof org.w3c.dom.Text) {
								continue;
							}

							if (!(failuresChild instanceof Element)) {
								throw new Exception("Expected type element as child of element 'failures'");
							}

							final Element failuresElement = (Element) failuresChild;

							if (!"failure".equals(failuresElement.getNodeName())) {
								throw new Exception("Expected element 'failure' as child of element 'failures'");
							}

							final String type = failuresElement.getAttribute("type");
							if (type == null) {
								throw new Exception("Element 'failure' lacks the attribute 'type'");
							}

							final boolean taskFailure = ("task".equals(type));
							String name = null;
							String interval = null;

							final NodeList failureChildren = failuresElement.getChildNodes();
							for (int k = 0; k < failureChildren.getLength(); ++k) {

								final Node failureChild = failureChildren.item(k);

								if (failureChild instanceof org.w3c.dom.Text) {
									continue;
								}

								if (!(failureChild instanceof Element)) {
									throw new Exception("Expected type element as child of element 'failure'");
								}

								final Element failureElement = (Element) failureChild;
								if ("name".equals(failureElement.getNodeName())) {
									name = extractValueFromElement(failureElement);
								}

								if ("interval".equals(failureElement.getNodeName())) {
									interval = extractValueFromElement(failureElement);
								}
							}

							if (name == null) {
								throw new Exception("Could not find name for failure event " + j);
							}

							if (interval == null) {
								throw new Exception("Could not find interval for failure event " + j);
							}

							int iv = 0;
							try {
								iv = Integer.parseInt(interval);

							} catch (NumberFormatException e) {
								throw new Exception("Interval " + interval + " for failure event " + j
									+ " is not an integer number");
							}

							if (iv <= 0) {
								throw new Exception("Interval for failure event " + j
									+ " must be greather than zero, but is " + iv);
							}

							AbstractFailureEvent failureEvent = null;
							if (taskFailure) {
								failureEvent = new VertexFailureEvent(name, iv);
							} else {
								failureEvent = new InstanceFailureEvent(name, iv);
							}

							jobFailurePattern.addEvent(failureEvent);
						}

						continue;
					}

					throw new Exception("Uncountered unecpted element " + patternElement.getNodeName());

				} else {
					throw new Exception("Encountered unexpected child of type " + patternChild.getClass());
				}
			}

		} catch (Exception e) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Cannot load failure pattern");
			messageBox.setMessage(e.getMessage());
			messageBox.open();
			return null;
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (Exception e) {
				}
			}
		}

		return jobFailurePattern;
	}

	private String extractValueFromElement(final Element element) throws Exception {

		final NodeList children = element.getChildNodes();
		if (children.getLength() != 1) {
			throw new Exception("Element " + element.getNodeName() + " has an unexpected number of children");
		}

		final Node child = children.item(0);

		if (!(child instanceof org.w3c.dom.Text)) {
			throw new Exception("Expected child of element " + element.getNodeName() + " to be of type text");
		}

		org.w3c.dom.Text childText = (org.w3c.dom.Text) child;

		return childText.getTextContent();
	}

	private void displayFailurePattern(final JobFailurePattern jobFailurePattern) {

		// Clear old content from event table
		this.failureEventTable.clearAll();

		if (jobFailurePattern == null) {
			this.failureEventTable.setEnabled(false);
			return;
		}

		this.failureEventTable.setEnabled(true);

		final Iterator<AbstractFailureEvent> it = jobFailurePattern.iterator();
		while (it.hasNext()) {

			final AbstractFailureEvent event = it.next();
			final TableItem ti = new TableItem(this.failureEventTable, SWT.NONE);
			updateTableItem(ti, event);
		}

		// Finally, add item to create new entry in both tables
		new TableItem(this.failureEventTable, SWT.NONE);

		this.selectedFailurePattern = jobFailurePattern;
	}

	public void startFailurePattern(final JobID jobID, final String jobName, final long referenceTime) {

		final JobFailurePattern failurePattern = this.failurePatterns.get(jobName.toLowerCase());
		if (failurePattern == null) {
			LOG.info("No failure pattern for job " + jobName);
		}

		final JobFailurePatternExecutor executor = new JobFailurePatternExecutor(this.shell.getDisplay(), jobID,
			jobName, failurePattern);

		executor.start(referenceTime);
	}
}
