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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Shell;

public abstract class AbstractSWTComponent {

	private final List<AbstractSWTComponent> children = new ArrayList<AbstractSWTComponent>();

	private final AbstractSWTComponent parent;

	protected final Rectangle rect = new Rectangle(0, 0, 0, 0);

	private boolean isVisible = true;

	protected AbstractSWTComponent(AbstractSWTComponent parent) {
		this.parent = parent;
		if (this.parent != null) {
			this.parent.addChild(this);
		}
	}

	protected void addChild(AbstractSWTComponent child) {
		if (!this.children.contains(child)) {
			this.children.add(child);
		}
	}

	public int getNumberOfChildren() {
		return this.children.size();
	}

	public Iterator<AbstractSWTComponent> getChildren() {
		return this.children.iterator();
	}

	public AbstractSWTComponent getChildAt(int index) {

		if (index >= this.children.size()) {
			return null;
		}

		return this.children.get(index);
	}

	public AbstractSWTComponent getParent() {
		return this.parent;
	}

	public int getX() {
		return this.rect.x;
	}

	public void setX(int x) {
		this.rect.x = x;
	}

	public int getY() {
		return this.rect.y;
	}

	public void setY(int y) {
		this.rect.y = y;
	}

	public int getWidth() {
		return this.rect.width;
	}

	public void setWidth(int width) {
		this.rect.width = width;
	}

	public int getHeight() {
		return this.rect.height;
	}

	public void setHeight(int height) {
		this.rect.height = height;
	}

	public abstract void layout();

	public void layoutChildren() {

		for (AbstractSWTComponent child : this.children) {
			child.layout();
		}
	}

	public final void paint(GC gc, Device device) {

		paintInternal(gc, device);

		for (AbstractSWTComponent child : this.children) {
			if (child.isVisible()) {
				child.paint(gc, device);
			}
		}
	}

	protected abstract void paintInternal(GC gc, Device device);

	public AbstractSWTComponent getSelectedComponent(int x, int y) {

		final Iterator<AbstractSWTComponent> it = getChildren();

		while (it.hasNext()) {

			AbstractSWTComponent child = it.next();

			AbstractSWTComponent selectedComponent = child.getSelectedComponent(x, y);
			if (selectedComponent != null) {
				return selectedComponent;
			}
		}

		return (isSelectable() && isWithinComponent(x, y)) ? this : null;
	}

	public SWTToolTip constructToolTip(Shell parentShell, SWTToolTipCommandReceiver commandReceiver, int x, int y) {
		return null;
	}

	public boolean isWithinComponent(int x, int y) {

		return this.rect.contains(x, y);
	}

	public abstract boolean isSelectable();

	public boolean isVisible() {
		return this.isVisible;
	}

	public void setVisible(boolean isVisible) {
		this.isVisible = isVisible;
	}
}
