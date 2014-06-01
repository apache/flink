/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.test.compiler.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.operators.BulkIteration;
import eu.stratosphere.api.java.record.operators.DeltaIteration;
import eu.stratosphere.util.Visitor;

/**
 * Utility to get operator instances from plans via name.
 */
public class OperatorResolver implements Visitor<Operator<?>> {
	
	private final Map<String, List<Operator<?>>> map;
	private Set<Operator<?>> seen;
	
	public OperatorResolver(Plan p) {
		this.map = new HashMap<String, List<Operator<?>>>();
		this.seen = new HashSet<Operator<?>>();
		
		p.accept(this);
		this.seen = null;
	}
	
	
	@SuppressWarnings("unchecked")
	public <T extends Operator<?>> T getNode(String name) {
		List<Operator<?>> nodes = this.map.get(name);
		if (nodes == null || nodes.isEmpty()) {
			throw new RuntimeException("No nodes found with the given name.");
		} else if (nodes.size() != 1) {
			throw new RuntimeException("Multiple nodes found with the given name.");
		} else {
			return (T) nodes.get(0);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Operator<?>> T getNode(String name, Class<? extends Function> stubClass) {
		List<Operator<?>> nodes = this.map.get(name);
		if (nodes == null || nodes.isEmpty()) {
			throw new RuntimeException("No node found with the given name and stub class.");
		} else {
			Operator<?> found = null;
			for (Operator<?> node : nodes) {
				if (node.getClass() == stubClass) {
					if (found == null) {
						found = node;
					} else {
						throw new RuntimeException("Multiple nodes found with the given name and stub class.");
					}
				}
			}
			if (found == null) {
				throw new RuntimeException("No node found with the given name and stub class.");
			} else {
				return (T) found;
			}
		}
	}
	
	public List<Operator<?>> getNodes(String name) {
		List<Operator<?>> nodes = this.map.get(name);
		if (nodes == null || nodes.isEmpty()) {
			throw new RuntimeException("No node found with the given name.");
		} else {
			return new ArrayList<Operator<?>>(nodes);
		}
	}

	@Override
	public boolean preVisit(Operator<?> visitable) {
		if (this.seen.add(visitable)) {
			// add to  the map
			final String name = visitable.getName();
			List<Operator<?>> list = this.map.get(name);
			if (list == null) {
				list = new ArrayList<Operator<?>>(2);
				this.map.put(name, list);
			}
			list.add(visitable);
			
			// recurse into bulk iterations
			if (visitable instanceof BulkIteration) {
				((BulkIteration) visitable).getNextPartialSolution().accept(this);
			} else if (visitable instanceof DeltaIteration) {
				((DeltaIteration) visitable).getSolutionSetDelta().accept(this);
				((DeltaIteration) visitable).getNextWorkset().accept(this);
			}
			
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void postVisit(Operator<?> visitable) {}
}
