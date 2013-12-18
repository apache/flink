/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.addons.visualization.swt;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;

public class SWTGraph extends AbstractSWTComponent {

	private final static int SPACING = 5;

	public SWTGraph() {
		super(null);

	}

	@Override
	public void layout() {

		int totalLongestPath = 0;
		int[] longestPath = new int[getNumberOfChildren()];

		for (int i = 0; i < getNumberOfChildren(); i++) {

			AbstractSWTComponent child = getChildAt(i);
			if (child instanceof AbstractSWTVertex) {
				AbstractSWTVertex childVertex = (AbstractSWTVertex) child;
				longestPath[i] = 1 + childVertex.getLongestPathAmongChildren();
			} else {
				longestPath[i] = 1;
			}
			totalLongestPath += longestPath[i];
		}

		if (totalLongestPath == 0) {
			return; // No children, so nothing to layout
		}

		int heightPerPathSegment = getHeight() / totalLongestPath;

		AbstractSWTComponent last = null;
		final int width = getWidth() - (2 * SPACING);
		final int x = getX() + SPACING;

		for (int i = 0; i < getNumberOfChildren(); i++) {

			AbstractSWTComponent child = getChildAt(i);
			child.setX(x);
			child.setWidth(width);
			final int height = (heightPerPathSegment * longestPath[i]) - (2 * SPACING);
			child.setHeight(height);
			if (last == null) {
				child.setY(getY() + getHeight() - child.getHeight() - SPACING);
			} else {
				child.setY(last.getY() - child.getHeight() - (SPACING * 2));
			}

			last = child;
		}

		layoutChildren();
	}

	@Override
	public boolean isSelectable() {
		return false;
	}

	@Override
	protected void paintInternal(GC gc, Device device) {

		gc.setBackground(ColorScheme.getGraphBackgroundColor(device));
		gc.fillRectangle(this.rect);
	}
}
