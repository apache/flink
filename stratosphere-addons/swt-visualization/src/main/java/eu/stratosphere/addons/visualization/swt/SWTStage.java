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

package eu.stratosphere.addons.visualization.swt;

import java.util.Iterator;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;

public class SWTStage extends AbstractSWTVertex {

	private final static int ARC_DIM = 10;

	private final static int SPACING = 20;

	private final static int NAMERECTWIDTH = 50;

	private AbstractSWTVertex[][] layoutMap = null;

	private int largestNumberOfParallelVertices = 1;

	private boolean uncluttered = false;

	public SWTStage(AbstractSWTComponent parent) {
		super(parent);
	}

	@Override
	public void layout() {

		if (this.layoutMap == null) {
			buildLayoutMap();
		}

		final int height = getHeight() / this.layoutMap.length - (2 * SPACING);
		final int width = (getWidth() - NAMERECTWIDTH) / this.largestNumberOfParallelVertices - (2 * SPACING);

		for (int i = 0; i < this.layoutMap.length; i++) {
			for (int j = 0; j < this.layoutMap[i].length; j++) {
				AbstractSWTVertex child = this.layoutMap[i][j];
				int offset = (this.largestNumberOfParallelVertices - this.layoutMap[i].length) * (width + 2 * SPACING)
					/ 2;
				final int y = getY() + (this.layoutMap.length - i - 1) * (height + (2 * SPACING)) + SPACING;
				final int x = getX() + NAMERECTWIDTH + offset + ((j + 1) * SPACING) + (j * (width + SPACING));
				child.setHeight(height);
				child.setWidth(width);
				child.setX(x);
				child.setY(y);
			}

		}

		if (this.layoutMap != null && !uncluttered && getWidth() > 0) {
			unclutterLayoutMap();
			uncluttered = true;
		}

		layoutChildren();
	}

	private void unclutterLayoutMap() {

		for (int i = 0; i < this.layoutMap.length - 1; i++) {

			int smallestLengthOfStage = getSumOfEdgeLengths(i);
			for (int j = 1; j < this.layoutMap[i].length; j++) {
				swapRects(i, j, j - 1);
				int tmp = getSumOfEdgeLengths(i);
				if (tmp < smallestLengthOfStage) {
					smallestLengthOfStage = tmp;
				} else {
					swapRects(i, j, j - 1); // Undo the change
				}
			}

		}

	}

	private void swapRects(int stage, int i, int j) {

		int swap_x = this.layoutMap[stage][i].getX();
		int swap_y = this.layoutMap[stage][i].getY();
		int swap_width = this.layoutMap[stage][i].getWidth();
		int swap_height = this.layoutMap[stage][i].getHeight();

		this.layoutMap[stage][i].setX(this.layoutMap[stage][j].getX());
		this.layoutMap[stage][i].setY(this.layoutMap[stage][j].getY());
		this.layoutMap[stage][i].setWidth(this.layoutMap[stage][j].getWidth());
		this.layoutMap[stage][i].setHeight(this.layoutMap[stage][j].getHeight());

		this.layoutMap[stage][j].setX(swap_x);
		this.layoutMap[stage][j].setY(swap_y);
		this.layoutMap[stage][j].setWidth(swap_width);
		this.layoutMap[stage][j].setHeight(swap_height);

	}

	private int getSumOfEdgeLengths(int lowerStage) {

		int sumOfEdgeLengths = 0;

		for (int i = 0; i < this.layoutMap[lowerStage].length; i++) {

			AbstractSWTVertex source = (AbstractSWTVertex) this.layoutMap[lowerStage][i];
			Iterator<AbstractSWTVertex> it = source.getEdges();
			while (it.hasNext()) {
				AbstractSWTVertex target = it.next();
				sumOfEdgeLengths += getLengthOfLine(source, target);
			}
		}

		return sumOfEdgeLengths;
	}

	private int getLengthOfLine(AbstractSWTVertex source, AbstractSWTVertex target) {

		int x = Math.abs(target.getEdgeTargetX() - source.getEdgeSourceX());
		int y = Math.abs(target.getEdgeTargetY() - source.getEdgeSourceY());

		return (int) Math.sqrt((x * x) + (y * y));
	}

	private void buildLayoutMap() {

		int longestPath = getLongestPathAmongChildren() + 1;
		int[] verticesPerSubStage = new int[longestPath];
		this.layoutMap = new AbstractSWTVertex[longestPath][];

		for (int i = 0; i < getNumberOfChildren(); i++) {
			AbstractSWTComponent child = getChildAt(i);
			if (child instanceof AbstractSWTVertex) {
				AbstractSWTVertex childVertex = (AbstractSWTVertex) child;
				verticesPerSubStage[longestPath - childVertex.getLongestPath(this) - 1]++;
			}
		}

		for (int i = 0; i < verticesPerSubStage.length; i++) {
			this.layoutMap[i] = new AbstractSWTVertex[verticesPerSubStage[i]];
			this.largestNumberOfParallelVertices = Math.max(this.largestNumberOfParallelVertices,
				verticesPerSubStage[i]);
			verticesPerSubStage[i] = 0;
		}

		for (int i = 0; i < getNumberOfChildren(); i++) {
			AbstractSWTComponent child = getChildAt(i);
			if (child instanceof AbstractSWTVertex) {
				AbstractSWTVertex childVertex = (AbstractSWTVertex) child;
				int index = longestPath - childVertex.getLongestPath(this) - 1;
				int alreadyAdded = verticesPerSubStage[index];
				this.layoutMap[index][alreadyAdded] = childVertex;
				verticesPerSubStage[index]++;
			}
		}
	}

	@Override
	public boolean isSelectable() {

		return false;
	}

	@Override
	protected void paintInternal(GC gc, Device device) {

		gc.setBackground(ColorScheme.getStageBackgroundColor(device));
		gc.fillRoundRectangle(this.rect.x, this.rect.y, this.rect.width, this.rect.height, ARC_DIM, ARC_DIM);
		gc.setForeground(ColorScheme.getStageBorderColor(device));
		gc.drawRoundRectangle(this.rect.x, this.rect.y, this.rect.width, this.rect.height, ARC_DIM, ARC_DIM);
	}
}
