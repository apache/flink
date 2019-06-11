/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.nodes.resource.batch.parallelism;

import org.apache.flink.table.plan.nodes.exec.ExecNode;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * There are no shuffle when transferring data in a shuffleStage.
 */
public class ShuffleStage {

	private final Set<ExecNode<?, ?>> execNodeSet = new LinkedHashSet<>();

	// parallelism of this shuffleStage.
	private int parallelism = -1;

	// whether this parallelism is final, if it is final, it can not be changed.
	private boolean isFinalParallelism = false;

	public void addNode(ExecNode<?, ?> node) {
		execNodeSet.add(node);
	}

	public void addNodeSet(Set<ExecNode<?, ?>> nodeSet) {
		execNodeSet.addAll(nodeSet);
	}

	public void removeNode(ExecNode<?, ?> node) {
		this.execNodeSet.remove(node);
	}

	public Set<ExecNode<?, ?>> getExecNodeSet() {
		return this.execNodeSet;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism, boolean finalParallelism) {
		if (this.isFinalParallelism) {
			if (finalParallelism && this.parallelism != parallelism) {
				throw new IllegalArgumentException("both fixed parallelism are not equal, old: " + this.parallelism + ", new: " + parallelism);
			}
		} else {
			if (finalParallelism) {
				this.parallelism = parallelism;
				this.isFinalParallelism = true;
			} else {
				this.parallelism = Math.max(this.parallelism, parallelism);
			}
		}
	}

	public boolean isFinalParallelism() {
		return isFinalParallelism;
	}

}
