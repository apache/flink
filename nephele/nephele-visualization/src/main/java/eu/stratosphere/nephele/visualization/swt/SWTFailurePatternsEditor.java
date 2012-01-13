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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TreeItem;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public final class SWTFailurePatternsEditor implements JobFailurePatternTreeListener {

	private static final int WIDTH = 800;

	private static final int HEIGHT = 400;

	private final Shell shell;

	private final SWTJobFailurePatternTree jobTree;

	private final SWTFailureEventTable failureEventTable;

	private final Set<String> jobSuggestions;

	private final Map<String, JobFailurePattern> loadedPatterns;

	SWTFailurePatternsEditor(final Shell parent, final Set<String> jobSuggestions, final Set<String> nameSuggestions,
			final Map<String, JobFailurePattern> loadedPatterns) {

		this.jobSuggestions = jobSuggestions;
		this.loadedPatterns = loadedPatterns;

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

		this.jobTree = new SWTJobFailurePatternTree(jobGroup, SWT.SINGLE | SWT.BORDER, this);

		this.failureEventTable = new SWTFailureEventTable(horizontalSash, SWT.BORDER | SWT.SINGLE, nameSuggestions);
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
		closeButton.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(SelectionEvent arg0) {

				shell.dispose();
			}
		});

		final Iterator<JobFailurePattern> it = this.loadedPatterns.values().iterator();
		while (it.hasNext()) {
			this.jobTree.addFailurePatternToTree(it.next());
		}

		// Initialize the tables
		if (this.jobTree.getItemCount() > 0) {

			final TreeItem ti = this.jobTree.getItem(0);
			this.jobTree.setSelection(ti);
			this.failureEventTable.showFailurePattern((JobFailurePattern) ti.getData());

		} else {
			this.failureEventTable.showFailurePattern(null);
		}

	}

	public Map<String, JobFailurePattern> show() {

		this.shell.open();

		final Display display = this.shell.getDisplay();

		while (!this.shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return this.loadedPatterns;
	}

	@Override
	public void addFailurePattern() {

		final Set<String> takenNames = this.loadedPatterns.keySet();

		final SWTNewFailurePatternDialog dialog = new SWTNewFailurePatternDialog(this.shell, this.jobSuggestions,
			takenNames);

		final String patternName = dialog.showDialog();
		if (patternName == null) {
			return;
		}

		final JobFailurePattern jobFailurePattern = new JobFailurePattern(patternName);

		// Add to loaded patterns
		this.loadedPatterns.put(jobFailurePattern.getName(), jobFailurePattern);

		this.jobTree.addFailurePatternToTree(jobFailurePattern);
		this.failureEventTable.showFailurePattern(jobFailurePattern);
	}

	@Override
	public void removeFailurePattern(final TreeItem selectedItem) {

		final JobFailurePattern failurePattern = (JobFailurePattern) selectedItem.getData();
		if (failurePattern == null) {
			return;
		}

		final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO);
		messageBox.setText("Confirm Removal");
		messageBox
			.setMessage("Do you really want to remove the job failure pattern '" + failurePattern.getName() + "'?");

		if (messageBox.open() != SWT.YES) {
			return;
		}

		selectedItem.dispose();

		this.loadedPatterns.remove(failurePattern.getName());

		if (this.jobTree.getItemCount() == 0) {
			jobFailurePatternSelected(null);
		} else {
			jobFailurePatternSelected(this.jobTree.getItem(0));
		}
	}

	@Override
	public void saveFailurePattern(final TreeItem selectedItem) {

		final JobFailurePattern failurePattern = (JobFailurePattern) selectedItem.getData();
		if (failurePattern == null) {
			return;
		}

		final FileDialog fileDialog = new FileDialog(this.shell, SWT.SAVE);
		fileDialog.setText("Save Failure Pattern");
		final String[] filterExts = { "*.xml", "*.*" };
		fileDialog.setFilterExtensions(filterExts);

		final String selectedFile = fileDialog.open();
		if (selectedFile == null) {
			return;
		}

		final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();

		try {
			final DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();

			final Document doc = builder.newDocument();

			// Construct the DOM tree
			final Element root = doc.createElement("pattern");
			doc.appendChild(root);

			final Element name = doc.createElement("name");
			root.appendChild(name);
			name.appendChild(doc.createTextNode(failurePattern.getName()));

			final Element failures = doc.createElement("failures");
			root.appendChild(failures);

			final Iterator<AbstractFailureEvent> it = failurePattern.iterator();
			while (it.hasNext()) {

				final AbstractFailureEvent event = it.next();

				final Element failure = doc.createElement("failure");
				failure.setAttribute("type", (event instanceof VertexFailureEvent) ? "task" : "instance");

				final Element failureName = doc.createElement("name");
				failureName.appendChild(doc.createTextNode(event.getName()));
				failure.appendChild(failureName);

				final Element interval = doc.createElement("interval");
				interval.appendChild(doc.createTextNode(Integer.toString(event.getInterval())));
				failure.appendChild(interval);

				failures.appendChild(failure);
			}

			// Write the DOM tree to the chosen file
			final DOMSource domSource = new DOMSource(doc);
			final TransformerFactory transformerFactory = TransformerFactory.newInstance();

			final FileOutputStream fos = new FileOutputStream(selectedFile);
			final Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.transform(domSource, new StreamResult(fos));

		} catch (Exception e) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Cannot load failure pattern");
			messageBox.setMessage(e.getMessage());
			messageBox.open();
		}

	}

	@Override
	public void jobFailurePatternSelected(final TreeItem selectedItem) {

		if (selectedItem == null) {
			this.failureEventTable.showFailurePattern(null);
		} else {
			this.failureEventTable.showFailurePattern((JobFailurePattern) selectedItem.getData());
		}
	}

	@Override
	public void loadFailurePattern() {

		final FileDialog fileDialog = new FileDialog(this.shell, SWT.OPEN);
		fileDialog.setText("Load Failure Pattern");
		final String[] filterExts = { "*.xml", "*.*" };
		fileDialog.setFilterExtensions(filterExts);

		final String selectedFile = fileDialog.open();
		if (selectedFile == null) {
			return;
		}

		final JobFailurePattern failurePattern = loadFailurePatternFromFile(selectedFile);

		if (this.loadedPatterns.containsKey(failurePattern.getName())) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Cannot load failure pattern");
			messageBox.setMessage("There is already a failure pattern loaded with the name '"
				+ failurePattern.getName() + "'. Please remove it first.");
			messageBox.open();

			return;
		}

		this.loadedPatterns.put(failurePattern.getName(), failurePattern);

		this.jobTree.addFailurePatternToTree(failurePattern);
		this.failureEventTable.showFailurePattern(failurePattern);
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
}
