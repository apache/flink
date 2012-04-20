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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractSWTVertex extends AbstractSWTComponent {

	private final List<AbstractSWTVertex> edges = new ArrayList<AbstractSWTVertex>();

	private int longestPath = -1;

	protected AbstractSWTVertex(AbstractSWTComponent parent) {
		super(parent);
	}

	Iterator<AbstractSWTVertex> getEdges() {
		return this.edges.iterator();
	}

	public void connectTo(AbstractSWTVertex vertex) {

		if (vertex == null) {
			return;
		}

		if (!this.edges.contains(vertex)) {
			this.edges.add(vertex);
		}
	}

	public int getLongestPathAmongChildren() {

		int longestPath = 0;
		Iterator<AbstractSWTComponent> it = getChildren();

		while (it.hasNext()) {

			AbstractSWTComponent child = it.next();

			if (child instanceof AbstractSWTVertex) {
				AbstractSWTVertex childVertex = (AbstractSWTVertex) child;
				longestPath = Math.max(longestPath, childVertex.getLongestPath(this));
			}
		}

		return longestPath;
	}

	public int getLongestPath(AbstractSWTVertex parent) {

		if (this.longestPath == -1) {

			int longestPath = 0;

			for (AbstractSWTVertex targetVertex : edges) {
				if (targetVertex.getParent() == parent) {
					longestPath = Math.max(longestPath, targetVertex.getLongestPath(parent) + 1);
				}
			}

			this.longestPath = longestPath;
		}

		return this.longestPath;
	}

	public int getEdgeSourceX() {
		return getX() + (getWidth() / 2);
	}

	public int getEdgeSourceY() {
		return getY();
	}

	public int getEdgeTargetX() {
		return getX() + (getWidth() / 2);
	}

	public int getEdgeTargetY() {
		return getY() + getHeight();
	}
}
