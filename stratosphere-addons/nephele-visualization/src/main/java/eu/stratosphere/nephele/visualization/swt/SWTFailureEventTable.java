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

import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MenuAdapter;
import org.eclipse.swt.events.MenuEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

import eu.stratosphere.util.StringUtils;

public final class SWTFailureEventTable extends Composite {

	private static final Log LOG = LogFactory.getLog(SWTFailureEventTable.class);

	private static final int DEFAULT_ICON_SIZE = 16;

	private static final int ICON_COLUMN_WIDTH = 20;

	private static final int TEXT_COLUMN_WIDTH = 200;

	private static Image TASK_ICON = null;

	private static Image INSTANCE_ICON = null;

	private final Table failureEventTable;

	private final Set<String> nameSuggestions;

	private JobFailurePattern selectedFailurePattern = null;

	public SWTFailureEventTable(final Composite parent, final int style, final Set<String> nameSuggestions) {
		super(parent, SWT.NONE);

		// Load images
		synchronized (SWTFailureEventTable.class) {

			if (TASK_ICON == null) {
				TASK_ICON = loadIcon("/eu/stratosphere/nephele/visualization/swt/taskicon.png");
			}

			if (INSTANCE_ICON == null) {
				INSTANCE_ICON = loadIcon("/eu/stratosphere/nephele/visualization/swt/nodeicon.png");
			}
		}

		this.nameSuggestions = nameSuggestions;

		setLayout(new FillLayout());

		this.failureEventTable = new Table(this, style);
		this.failureEventTable.setHeaderVisible(true);
		this.failureEventTable.setLinesVisible(true);

		final TableColumn iconColumn = new TableColumn(this.failureEventTable, SWT.NONE);
		final TableColumn nameColumn = new TableColumn(this.failureEventTable, SWT.NONE);
		nameColumn.setText("Name");
		final TableColumn intervalColumn = new TableColumn(this.failureEventTable, SWT.NONE);
		intervalColumn.setText("Interval");

		for (int i = 0; i < this.failureEventTable.getColumnCount(); ++i) {
			if (i == 0) {
				this.failureEventTable.getColumn(i).setWidth(ICON_COLUMN_WIDTH);
			} else {
				this.failureEventTable.getColumn(i).setWidth(TEXT_COLUMN_WIDTH);
			}
		}

		// Implement listener to add and update events
		this.failureEventTable.addMouseListener(new MouseAdapter() {

			@Override
			public void mouseDoubleClick(final MouseEvent arg0) {

				final TableItem ti = failureEventTable.getItem(new Point(arg0.x, arg0.y));

				if (selectedFailurePattern == null) {
					return;
				}

				addOrEditTableItem(ti);
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
					failureEventTable.setSortColumn(currentColumn);
					dir = SWT.UP;
				}

				final int direction = dir;
				final AbstractFailureEvent[] failureEvents = new AbstractFailureEvent[failureEventTable.getItemCount()];
				for (int i = 0; i < failureEventTable.getItemCount(); ++i) {
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

		// Implement keyboard commands
		this.failureEventTable.addKeyListener(new KeyAdapter() {

			@Override
			public void keyReleased(final KeyEvent arg0) {

				if (arg0.keyCode == SWT.DEL) {
					removeSelectedTableItems();
				} else if (arg0.keyCode == SWT.CR) {
					addOrEditSelectedTableItems();
				}
			}
		});

		// Set the menu
		this.failureEventTable.setMenu(createTableContextMenu());
	}

	private Image loadIcon(final String path) {

		final Display display = getDisplay();
		final InputStream in = getClass().getResourceAsStream(path);
		try {
			return new Image(display, in);
		} catch (Exception e) {
			LOG.warn(StringUtils.stringifyException(e));
		}

		final Image image = new Image(display, DEFAULT_ICON_SIZE, DEFAULT_ICON_SIZE);
		final GC gc = new GC(image);
		gc.setBackground(display.getSystemColor(SWT.COLOR_WHITE));
		gc.fillRectangle(image.getBounds());
		gc.dispose();

		return image;
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
				ti.setImage(TASK_ICON);
			} else {
				ti.setImage(INSTANCE_ICON);
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

	private Menu createTableContextMenu() {

		final Menu tableContextMenu = new Menu(getShell());

		final MenuItem addItem = new MenuItem(tableContextMenu, SWT.PUSH);
		addItem.setText("Add...");
		addItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				addOrEditSelectedTableItems();
			}

		});

		final MenuItem editItem = new MenuItem(tableContextMenu, SWT.PUSH);
		editItem.setText("Edit...");
		editItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				addOrEditSelectedTableItems();
			}

		});

		final MenuItem removeItem = new MenuItem(tableContextMenu, SWT.PUSH);
		removeItem.setText("Remove...");
		removeItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				removeSelectedTableItems();
			}
		});

		tableContextMenu.addMenuListener(new MenuAdapter() {

			@Override
			public void menuShown(final MenuEvent arg0) {

				TableItem[] selectedItems = failureEventTable.getSelection();
				if (selectedItems == null) {

					return;
				}

				if (selectedItems.length == 0) {

					editItem.setEnabled(false);
					removeItem.setEnabled(false);

					return;
				}

				if (selectedItems[0].getData() == null) {

					editItem.setEnabled(false);
					removeItem.setEnabled(false);

					return;
				}

				editItem.setEnabled(true);
				removeItem.setEnabled(true);
			}
		});

		return tableContextMenu;
	}

	private void addOrEditSelectedTableItems() {

		final TableItem[] selectedItems = this.failureEventTable.getSelection();
		if (selectedItems == null) {
			return;
		}

		for (final TableItem selectedItem : selectedItems) {
			addOrEditTableItem(selectedItem);
		}
	}

	private void addOrEditTableItem(final TableItem ti) {

		AbstractFailureEvent oldEvent = null;
		if (ti != null) {
			oldEvent = (AbstractFailureEvent) ti.getData();
		}

		final SWTFailureEventEditor editor = new SWTFailureEventEditor(getShell(), nameSuggestions, oldEvent);

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

	private void removeSelectedTableItems() {

		final TableItem[] selectedItems = this.failureEventTable.getSelection();
		if (selectedItems == null) {
			return;
		}

		for (final TableItem selectedItem : selectedItems) {
			removeTableItem(selectedItem);
		}
	}

	private void removeTableItem(final TableItem ti) {

		final AbstractFailureEvent event = (AbstractFailureEvent) ti.getData();
		if (event == null) {
			return;
		}

		final MessageBox messageBox = new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
		messageBox.setText("Confirm removal");
		messageBox.setMessage("Do you really want to remove the event '" + event.getName() + "'");
		if (messageBox.open() == SWT.YES) {
			ti.dispose();
		}

		this.selectedFailurePattern.removeEvent(event);
	}

	public void showFailurePattern(final JobFailurePattern jobFailurePattern) {

		// Clear old content from event table
		this.failureEventTable.removeAll();

		if (jobFailurePattern == null) {
			this.failureEventTable.setEnabled(false);
			return;
		}

		this.failureEventTable.setEnabled(true);

		final Iterator<AbstractFailureEvent> it = jobFailurePattern.iterator();
		while (it.hasNext()) {

			final AbstractFailureEvent event = it.next();
			final TableItem ti = new TableItem(this.failureEventTable, SWT.NONE);
			ti.setData(event);
			updateTableItem(ti, event);
		}

		// Finally, add item to create new entry in both tables
		new TableItem(this.failureEventTable, SWT.NONE);

		this.selectedFailurePattern = jobFailurePattern;
	}
}
