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
package eu.stratosphere.util.dag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Arvid Heise
 */
public class GraphTest {
	@Test
	public void testIteratorOnDiamond() {
		final SimpleNode in = new SimpleNode("in");
		final SimpleNode a = new SimpleNode("a", in);
		final SimpleNode b = new SimpleNode("b", in);
		final SimpleNode out = new SimpleNode("out", a, b);

		final Graph<SimpleNode> graph = new Graph<SimpleNode>(new SimpleNodeModifier(), in);

		final ArrayList<SimpleNode> retrievedNodes = new ArrayList<SimpleNode>();
		for (final Graph<SimpleNode>.NodePath nodePath : graph)
			retrievedNodes.add(nodePath.getNode());

		final List<SimpleNode> expected = Arrays.asList(in, a, out, b, out);

		Assert.assertEquals(expected, retrievedNodes);
	}

	@Test
	public void testIteratorWithTwoSources() {
		final SimpleNode in = new SimpleNode("in");
		final SimpleNode a = new SimpleNode("a", in);

		final SimpleNode in2 = new SimpleNode("in2");
		final SimpleNode b = new SimpleNode("b", in2);
		final SimpleNode out = new SimpleNode("out", a, b);

		final Graph<SimpleNode> graph = new Graph<SimpleNode>(new SimpleNodeModifier(), in, in2);

		final ArrayList<SimpleNode> retrievedNodes = new ArrayList<SimpleNode>();
		for (final Graph<SimpleNode>.NodePath nodePath : graph)
			retrievedNodes.add(nodePath.getNode());

		final List<SimpleNode> expected = Arrays.asList(in, a, out, in2, b, out);

		Assert.assertEquals(expected, retrievedNodes);
	}

	@Test
	public void testIteratorWithTwoSources2() {
		final SimpleNode in = new SimpleNode("in");
		final SimpleNode a = new SimpleNode("a", in);
		final SimpleNode b = new SimpleNode("b", in);
		final SimpleNode out = new SimpleNode("out", a, b);
		final SimpleNode in2 = new SimpleNode("in2");
		in2.outgoings.add(b);

		final Graph<SimpleNode> graph = new Graph<SimpleNode>(new SimpleNodeModifier(), in, in2);

		final ArrayList<SimpleNode> retrievedNodes = new ArrayList<SimpleNode>();
		for (final Graph<SimpleNode>.NodePath nodePath : graph)
			retrievedNodes.add(nodePath.getNode());

		final List<SimpleNode> expected = Arrays.asList(in, a, out, b, out, in2, b, out);

		Assert.assertEquals(expected, retrievedNodes);
	}

	@Test
	public void testFindAll() {
		final SimpleNode in = new SimpleNode("in");
		final SimpleNode a = new SimpleNode("a", in);
		final SimpleNode b = new SimpleNode("b", in);
		final SimpleNode out = new SimpleNode("out", a, b);
		final SimpleNode in2 = new SimpleNode("in2");
		in2.outgoings.add(b);

		final Graph<SimpleNode> graph = new Graph<SimpleNode>(new SimpleNodeModifier(), in, in2);

		final ArrayList<Graph<SimpleNode>.NodePath> retrievedNodes = new ArrayList<Graph<SimpleNode>.NodePath>();
		for (final Graph<SimpleNode>.NodePath nodePath : graph.findAll(out, false))
			retrievedNodes.add(nodePath);

		final List<Graph<SimpleNode>.NodePath> expected = new ArrayList<Graph<SimpleNode>.NodePath>();
		expected.add(graph.getPath(in, 0, 0));
		expected.add(graph.getPath(in, 1, 0));
		expected.add(graph.getPath(in2, 0, 0));

		Assert.assertEquals(expected, retrievedNodes);
	}

	@Test
	public void testReplace() {
		final SimpleNode in = new SimpleNode("in");
		final SimpleNode a = new SimpleNode("a", in);
		final SimpleNode b = new SimpleNode("b", in);
		final SimpleNode out = new SimpleNode("out", a, b);
		final SimpleNode in2 = new SimpleNode("in2");
		in2.outgoings.add(b);

		final Graph<SimpleNode> graph = new Graph<SimpleNode>(new SimpleNodeModifier(), in, in2);

		final SimpleNode replacement = new SimpleNode("replaced");
		graph.replace(out, replacement, false);

		final ArrayList<SimpleNode> retrievedNodes = new ArrayList<SimpleNode>();
		for (final Graph<SimpleNode>.NodePath nodePath : graph)
			retrievedNodes.add(nodePath.getNode());

		final List<SimpleNode> expected = Arrays.asList(in, a, replacement, b, replacement, in2, b, replacement);

		Assert.assertEquals(expected, retrievedNodes);
	}

	static class SimpleNodeModifier implements ConnectionModifier<SimpleNode> {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.dag.ConnectionNavigator#getConnectedNodes(java.lang.Object)
		 */
		@Override
		public List<? extends SimpleNode> getConnectedNodes(final SimpleNode node) {
			return node.getOutgoings();
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.dag.ConnectionModifier#setConnectedNodes(java.lang.Object, java.util.List)
		 */
		@Override
		public void setConnectedNodes(final SimpleNode parent, final List<SimpleNode> children) {
			parent.setOutgoings(children);
		}
	}

	static class SimpleNode {
		private List<SimpleNode> outgoings = new ArrayList<SimpleNode>();

		private final String label;

		public SimpleNode(final String label, final SimpleNode... incomings) {
			this.label = label;
			for (int index = 0; index < incomings.length; index++)
				incomings[index].outgoings.add(this);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return this.label;
		}

		/**
		 * Returns the outgoings.
		 * 
		 * @return the outgoings
		 */
		public List<SimpleNode> getOutgoings() {
			return this.outgoings;
		}

		/**
		 * Sets the outgoings to the specified value.
		 * 
		 * @param outgoings
		 *        the outgoings to set
		 */
		public void setOutgoings(final List<SimpleNode> outgoings) {
			if (outgoings == null)
				throw new NullPointerException("outgoings must not be null");

			this.outgoings = outgoings;
		}
	}
}
