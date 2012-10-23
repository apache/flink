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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

public final class SWTJobFailurePatternTree extends Composite {

	private final Tree jobTree;

	private final MenuItem addItem;

	private final MenuItem removeItem;

	private final MenuItem saveItem;

	private final MenuItem loadItem;

	SWTJobFailurePatternTree(final Composite parent, final int style, final JobFailurePatternTreeListener treeListener) {
		super(parent, SWT.NONE);

		setLayout(new FillLayout());

		final Menu treeContextMenu = new Menu(getShell());
		this.addItem = new MenuItem(treeContextMenu, SWT.PUSH);
		this.addItem.setText("Add...");
		this.addItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				treeListener.addFailurePattern();
			}
		});

		new MenuItem(treeContextMenu, SWT.SEPARATOR);

		this.removeItem = new MenuItem(treeContextMenu, SWT.PUSH);
		this.removeItem.setText("Remove...");
		this.removeItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {

				final TreeItem[] selectedItems = jobTree.getSelection();
				if (selectedItems == null) {
					return;
				}

				for (final TreeItem selectedItem : selectedItems) {
					treeListener.removeFailurePattern(selectedItem);
				}
			}
		});
		new MenuItem(treeContextMenu, SWT.SEPARATOR);
		this.saveItem = new MenuItem(treeContextMenu, SWT.PUSH);
		this.saveItem.setText("Save...");
		this.saveItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {

				final TreeItem[] selectedItems = jobTree.getSelection();
				if (selectedItems == null) {
					return;
				}

				for (final TreeItem selectedItem : selectedItems) {
					treeListener.saveFailurePattern(selectedItem);
				}
			}
		});
		this.loadItem = new MenuItem(treeContextMenu, SWT.PUSH);
		this.loadItem.setText("Load...");
		this.loadItem.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {
				treeListener.loadFailurePattern();
			}
		});

		this.jobTree = new Tree(this, style);
		this.jobTree.setMenu(treeContextMenu);
		this.jobTree.addMouseListener(new MouseAdapter() {

			@Override
			public void mouseDown(final MouseEvent arg0) {

				final boolean itemSelected = (jobTree.getItem(new Point(arg0.x, arg0.y)) != null);
				addItem.setEnabled(!itemSelected);
				removeItem.setEnabled(itemSelected);
				saveItem.setEnabled(itemSelected);
				loadItem.setEnabled(!itemSelected);
			}
		});

		this.jobTree.addKeyListener(new KeyAdapter() {

			@Override
			public void keyReleased(final KeyEvent arg0) {

				if (arg0.keyCode == SWT.DEL) {

					final TreeItem[] selectedItems = jobTree.getSelection();
					if (selectedItems == null) {
						return;
					}

					for (final TreeItem selectedItem : selectedItems) {
						treeListener.removeFailurePattern(selectedItem);
					}
				}
			}

		});

		this.jobTree.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(final SelectionEvent arg0) {

				final TreeItem[] selectedItems = jobTree.getSelection();

				if (selectedItems == null) {
					return;
				}

				for (final TreeItem selectedItem : selectedItems) {
					treeListener.jobFailurePatternSelected(selectedItem);
				}
			}
		});
	}

	public int getItemCount() {

		return this.jobTree.getItemCount();
	}

	public TreeItem getItem(final int index) {

		return this.jobTree.getItem(index);
	}

	public void setSelection(final TreeItem treeItem) {

		this.jobTree.setSelection(treeItem);
	}

	public TreeItem[] getSelection() {

		return this.jobTree.getSelection();
	}

	public void addFailurePatternToTree(final JobFailurePattern failurePattern) {

		final TreeItem jobFailureItem = new TreeItem(this.jobTree, SWT.NONE);
		jobFailureItem.setText(failurePattern.getName());
		jobFailureItem.setData(failurePattern);
		
		this.jobTree.setSelection(jobFailureItem);
	}
}
