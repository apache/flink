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
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.util.AbstractIterator;
import eu.stratosphere.util.ConversionIterator;
import eu.stratosphere.util.FilteringIterable;
import eu.stratosphere.util.Predicate;

/**
 * @author Arvid Heise
 */
public class Graph<Node> implements Iterable<Graph<Node>.NodePath> {
	private List<Node> startNodes = new ArrayList<Node>();

	private ConnectionNavigator<Node> navigator;

	private ConnectionModifier<Node> modifier;

	private final RootPath root = new RootPath();

	/**
	 * Initializes Graph.
	 */
	public Graph(final ConnectionModifier<Node> modifier, final List<Node> startNodes) {
		this.startNodes = startNodes;
		this.navigator = this.modifier = modifier;
	}

	public Graph(final ConnectionModifier<Node> modifier, final Node... startNodes) {
		this(modifier, Arrays.asList(startNodes));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Graph other = (Graph) obj;
		return this.startNodes.equals(other.startNodes);
	}

	public Iterable<Graph<Node>.NodePath> findAll(final Node toFind, final boolean equal) {
		final Predicate<Graph<Node>.NodePath> selector = equal ?
			new Predicate<Graph<Node>.NodePath>() {
				@Override
				public boolean isTrue(final Graph<Node>.NodePath param) {
					return param.equals(toFind);
				};
			} :
			new Predicate<Graph<Node>.NodePath>() {
				@Override
				public boolean isTrue(final Graph<Node>.NodePath param) {
					return param.getNode() == toFind;
				};
			};
		return new FilteringIterable<Graph<Node>.NodePath>(this, selector);
	}

	/**
	 * Returns the startNodes.
	 * 
	 * @return the startNodes
	 */
	public List<Node> getStartNodes() {
		return this.startNodes;
	}

	public NodePath getPath(final Node startNode, final int... indizes) {
		int startIndex = -1;
		for (int index = 0; index < this.startNodes.size(); index++)
			if (this.startNodes.get(index) == startNode) {
				startIndex = index;
				break;
			}

		if (startIndex == -1)
			throw new IllegalArgumentException("unknown start node");

		NodePath nodePath = new NodePath(this.root, startNode, startIndex);
		for (final int pathIndex : indizes)
			nodePath = nodePath.followConnection(pathIndex);
		return nodePath;
	}

	public Iterable<Graph<Node>.NodePath> findAll(final Predicate<Graph<Node>.NodePath> predicate) {
		return new FilteringIterable<Graph<Node>.NodePath>(this, predicate);
	}

	public Iterable<Graph<Node>.NodePath> findAllIncomings(final Node node) {
		return new FilteringIterable<Graph<Node>.NodePath>(this, new Predicate<Graph<Node>.NodePath>() {
			@Override
			public boolean isTrue(final Graph<Node>.NodePath param) {
				for (final Node outgoing : param.getOutgoings())
					if (outgoing == node)
						return true;
				return false;
			};
		});
	}

	public Iterable<Graph<Node>.NodePath> findAllTypes(final Class<?> clazz) {
		return new FilteringIterable<Graph<Node>.NodePath>(this, new Predicate<Graph<Node>.NodePath>() {
			@Override
			public boolean isTrue(final Graph<Node>.NodePath param) {
				return clazz.isInstance(param);
			};
		});
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.startNodes.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Graph<Node>.NodePath> iterator() {
		return new GraphIterator();
	}

	public void replace(final Node toReplace, final Node replacement, final boolean equal) {
		for (final NodePath replaceNode : this.findAll(toReplace, equal)) {
			final int index = replaceNode.getIndex();
			final NodePath incoming = replaceNode.getIncoming();
			this.replaceChild(incoming, index, replacement);
		}
	}

	public void replaceChild(final NodePath referencingNode, final int index, final Node replacement) {
		final List<Node> outgoings = referencingNode.getOutgoings();
		outgoings.set(index, replacement);
		referencingNode.setOutgoings(outgoings);
	}

	/**
	 * @author Arvid Heise
	 */
	public class GraphIterator extends AbstractIterator<NodePath> {
		private NodePath path = new StartPath();

		private NodePath followNext(final NodePath path, final int nextIndex) {
			final NodePath incoming = path.getIncoming();
			if (incoming == null)
				return this.noMoreElements();

			if (nextIndex < incoming.getOutgoingCount())
				return incoming.followConnection(nextIndex);

			return this.followNext(incoming, incoming.getIndex() + 1);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.AbstractIterator#loadNext()
		 */
		@Override
		protected NodePath loadNext() {
			if (this.path.getOutgoingCount() > 0)
				return this.path = this.path.followConnection(0);

			return this.path = this.followNext(this.path, this.path.getIndex() + 1);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.AbstractIterator#remove()
		 */
		@Override
		public void remove() {
			final NodePath referencingNode = this.path.getIncoming();
			final List<Node> outgoings = referencingNode.getOutgoings();
			outgoings.remove(this.path.getIndex());
			referencingNode.setOutgoings(outgoings);
		}

		private class StartPath extends NodePath {
			/**
			 * Initializes Graph.GraphIterator.StartPath.
			 */
			public StartPath() {
				super(Graph.this.root, null, -1);
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.dag.Graph.NodePath#getOutgoingCount()
			 */
			@Override
			public int getOutgoingCount() {
				return 0;
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.dag.Graph.NodePath#toString()
			 */
			@Override
			public String toString() {
				return "start";
			}
		}
	}

	public class NodePath implements Iterable<NodePath> {
		private NodePath parentPath;

		private final Node node;

		private int index;

		private NodePath(final Node node) {
			this.node = node;
		}

		private NodePath(final NodePath parentPath, final Node node, final int followedIndex) {
			this.parentPath = parentPath;
			this.node = node;
			this.index = followedIndex;
		}

		public NodePath followConnection(final int connectionIndex) {
			return new NodePath(this, Graph.this.navigator.getConnectedNodes(this.node).get(connectionIndex),
				connectionIndex);
		}

		public Iterable<NodePath> getAllIncoming() {
			return this.parentPath;
		}

		public NodePath getIncoming() {
			return this.parentPath;
		}

		/**
		 * Returns the followedIndex.
		 * 
		 * @return the followedIndex
		 */
		public int getIndex() {
			return this.index;
		}

		/**
		 * Returns the node.
		 * 
		 * @return the node
		 */
		public Node getNode() {
			return this.node;
		}

		public int getOutgoingCount() {
			return Graph.this.navigator.getConnectedNodes(this.node).size();
		}

		@SuppressWarnings("unchecked")
		public List<Node> getOutgoings() {
			return (List<Node>) Graph.this.navigator.getConnectedNodes(this.node);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Iterable#iterator()
		 */
		@Override
		public Iterator<NodePath> iterator() {
			return new ConversionIterator<Node, NodePath>(Graph.this.navigator.getConnectedNodes(this.node).iterator()) {
				int outIndex = 0;

				@Override
				protected NodePath convert(final Node inputObject) {
					return new NodePath(NodePath.this, inputObject, this.outIndex++);
				};
			};
		}

		public void setOutgoings(final List<Node> outgoings) {
			Graph.this.modifier.setConnectedNodes(this.node, outgoings);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			final StringBuilder builder = new StringBuilder();
			if (this.parentPath != null)
				builder.append(this.parentPath.toString()).append("[").append(this.index).append("]=>");
			builder.append(this.node.toString());
			return builder.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.getOuterType().hashCode();
			result = prime * result + this.index;
			result = prime * result + (this.node == null ? 0 : this.node.hashCode());
			result = prime * result + (this.parentPath == null ? 0 : this.parentPath.hashCode());
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			@SuppressWarnings("unchecked")
			final NodePath other = (NodePath) obj;
			return this.getOuterType().equals(other.getOuterType())
				&& this.index == other.index
				&& (this.node == null ? other.node == null : this.node.equals(other.node))
				&& (this.parentPath == null ? other.parentPath == null : this.parentPath.equals(other.parentPath));
		}

		private Graph<Node> getOuterType() {
			return Graph.this;
		}
	}

	private class RootPath extends NodePath {
		/**
		 * Initializes Graph.RootPath.
		 */
		public RootPath() {
			super(null);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.dag.Graph.NodePath#followConnection(int)
		 */
		@Override
		public NodePath followConnection(final int connectionIndex) {
			return new NodePath(this, Graph.this.startNodes.get(connectionIndex), connectionIndex);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.dag.Graph.NodePath#getOutgoingCount()
		 */
		@Override
		public int getOutgoingCount() {
			return Graph.this.startNodes.size();
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.dag.Graph.NodePath#toString()
		 */
		@Override
		public String toString() {
			return "root";
		}
	}
}
