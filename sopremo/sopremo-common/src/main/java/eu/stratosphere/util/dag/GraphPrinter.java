package eu.stratosphere.util.dag;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.util.dag.GraphLevelPartitioner.Level;

/**
 * Utility class to pretty print arbitrary directed acyclic graphs. It needs a {@link Navigator} to traverse form the
 * start nodes through the graph and optionally a {@link NodePrinter} to format the nodes.<br>
 * <br>
 * The nodes are printed in rows visualizing the dependencies. A dependency of node A is a node B iff there exists an
 * edge from B to A. The entries in the last row have no incoming edges and thus dependencies. The entries on the next
 * to last row only depend on nodes on the last row.<br>
 * All nodes have a maximum width that can be adjusted ({@link #setWidth(int)}). <br>
 * To visualize the edges, a {@link ConnectorProvider} provides printable strings between the nodes.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the node
 */
public class GraphPrinter<Node> {
	/**
	 * The default width of a column.
	 */
	public static final int DEFAULT_COLUMN_WIDTH = 20;

	private NodePrinter<Node> nodePrinter = new StandardPrinter<Node>();

	private int width = DEFAULT_COLUMN_WIDTH;

	private String widthString = "%-" + this.width + "s";

	private ConnectorProvider connectorProvider = new BoxConnectorProvider();

	/**
	 * Returns the {@link ConnectorProvider} that provides printable strings for the edges between the nodes.
	 * 
	 * @return the connector provider
	 */
	public ConnectorProvider getConnectorProvider() {
		return this.connectorProvider;
	}

	/**
	 * Returns the {@link NodePrinter} responsible for formatting the nodes.
	 * 
	 * @return the node printer
	 */
	public NodePrinter<Node> getNodePrinter() {
		return this.nodePrinter;
	}

	/**
	 * Returns the maximum width of the nodes.
	 * 
	 * @return the maximum width
	 */
	public int getWidth() {
		return this.width;
	}

	/**
	 * Prints the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link Formatter} format and all structural elements are formatted with the given width.
	 * 
	 * @param appendable
	 *        the target of the print operations
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 * @throws IOException
	 *         if an I/O error occurred during the print operation
	 */
	public void print(Appendable appendable, Iterable<? extends Node> startNodes, Navigator<Node> navigator)
			throws IOException {
		this.print(appendable, startNodes.iterator(), navigator);
	}

	/**
	 * Prints the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link Formatter} format and all structural elements are formatted with the given width.
	 * 
	 * @param appendable
	 *        the target of the print operations
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 * @throws IOException
	 *         if an I/O error occurred during the print operation
	 */
	public void print(Appendable appendable, Iterator<? extends Node> startNodes, Navigator<Node> navigator)
			throws IOException {
		new PrintState(appendable, GraphLevelPartitioner.getLevels(startNodes, navigator)).printDAG();
	}

	/**
	 * Prints the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link Formatter} format and all structural elements are formatted with the given width.
	 * 
	 * @param appendable
	 *        the target of the print operations
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 * @throws IOException
	 *         if an I/O error occurred during the print operation
	 */
	public void print(Appendable appendable, Node[] startNodes, Navigator<Node> navigator) throws IOException {
		this.print(appendable, Arrays.asList(startNodes).iterator(), navigator);
	}

	/**
	 * Sets the {@link ConnectorProvider} providing printable strings for the edges between the nodes.
	 * 
	 * @param connectorProvider
	 *        the new connector provider
	 */
	public void setConnectorProvider(ConnectorProvider connectorProvider) {
		if (connectorProvider == null)
			throw new NullPointerException("connectorProvider must not be null");

		this.connectorProvider = connectorProvider;
	}

	/**
	 * Sets the {@link NodePrinter} responsible for formatting the nodes.
	 * 
	 * @param nodePrinter
	 *        the node printer
	 */
	public void setNodePrinter(NodePrinter<Node> nodePrinter) {
		if (nodePrinter == null)
			throw new NullPointerException("nodePrinter must not be null");

		this.nodePrinter = nodePrinter;
	}

	/**
	 * Sets the width of a column of nodes. The default width is
	 * 
	 * @param width
	 */
	public void setWidth(int width) {
		this.width = width;
		this.widthString = "%-" + width + "s";
	}

	/**
	 * Converts the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link NodePrinter} and all structural elements are formatted with the given width.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 * @return a string representation of the directed acyclic graph
	 */
	public String toString(Iterable<? extends Node> startNodes, Navigator<Node> navigator) {
		return this.toString(startNodes.iterator(), navigator);
	}

	/**
	 * Converts the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link NodePrinter} and all structural elements are formatted with the given width.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 * @return a string representation of the directed acyclic graph
	 */
	public String toString(Iterator<? extends Node> startNodes, Navigator<Node> navigator) {
		StringBuilder builder = new StringBuilder();

		try {
			this.print(builder, startNodes, navigator);
		} catch (IOException e) {
			// cannot happen since we use a StringBuilder
		}

		return builder.toString();
	}

	/**
	 * Converts the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link NodePrinter} and all structural elements are formatted with the given width.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 * @return a string representation of the directed acyclic graph
	 */
	public String toString(Node[] startNodes, Navigator<Node> navigator) {
		return this.toString(Arrays.asList(startNodes), navigator);
	}

	/**
	 * Formats node using a given format pattern and {@link String#format(String, Object...)}.
	 * 
	 * @author Arvid Heise
	 * @param <Node>
	 *        the class of the node
	 */
	public static class FormattedNodePrinter<Node> implements NodePrinter<Node> {
		private final String format;

		private FormattedNodePrinter(String format) {
			this.format = format;
		}

		@Override
		public String toString(Object node) {
			return String.format(this.format, node.toString());
		}
	}

	/**
	 * Placeholder to format connections between nodes above several levels. There are two types: spacers and
	 * connectors.
	 * 
	 * @author Arvid Heise
	 */
	private static class Placeholder {
		private List<Object> targets = new ArrayList<Object>(1);

		/**
		 * Initializes a spacer.
		 */
		public Placeholder() {
		}

		Placeholder(Object target) {
			this.targets.add(target);
		}

		public String toString(ConnectorProvider connectorProvider) {
			if (this.targets.isEmpty())
				return "";
			return connectorProvider.getConnectorString(ConnectorProvider.Route.TOP_DOWN);
			// return this.targets.isEmpty() ? "" : "|";
			// return this.targets.isEmpty() ? "" : String.valueOf(index) + targets.toString();
		}
	}

	private class PrintState {
		private Appendable appender;

		private List<Level<Object>> levels;

		private IntList printDownline = new IntArrayList();

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private PrintState(Appendable builder, List<Level<Node>> levels) {
			this.appender = builder;
			this.levels = (List) levels;
			this.addPlaceholders(this.levels);
		}

		public Placeholder addPlaceholder(Level<Object> level, int placeholderIndex, Object to) {
			for (int index = level.getLevelNodes().size(); index < placeholderIndex; index++) {
				Placeholder emptyPlaceholder = new Placeholder();
				level.add(emptyPlaceholder);
			}

			for (; placeholderIndex < level.getLevelNodes().size(); placeholderIndex++) {
				if (!(level.getLevelNodes().get(placeholderIndex) instanceof Placeholder))
					break;
				if (((Placeholder) level.getLevelNodes().get(placeholderIndex)).targets.isEmpty()) {
					level.getLevelNodes().remove(placeholderIndex);
					break;
				}
			}

			Placeholder placeholder = new Placeholder(to);
			level.add(placeholderIndex, placeholder);
			level.updateLink(placeholder, null, to);
			// level.getLevelNodes().add(placeholderIndex, placeholder);
			// this.outgoings.put(placeholder, new ArrayList<Object>(Arrays.asList(to)));
			return placeholder;
		}

		private void addPlaceholders(List<Level<Object>> levels) {
			for (int levelIndex = levels.size() - 1; levelIndex >= 0; levelIndex--) {
				Level<Object> level = levels.get(levelIndex);
				List<Object> levelNodes = level.getLevelNodes();

				int placeHolderIndex = 0;
				for (int nodeIndex = 0; nodeIndex < levelNodes.size(); nodeIndex++) {
					Object node = levelNodes.get(nodeIndex);

					List<Object> inputs = level.getLinks(node);

					for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++, placeHolderIndex++) {
						Object input = inputs.get(inputIndex);
						if (levels.get(levelIndex - 1).getLevelNodes().indexOf(input) == -1) {
							Object placeholder = this.addPlaceholder(levels.get(levelIndex - 1), placeHolderIndex,
								input);
							level.updateLink(node, input, placeholder);
						}
					}
				}
			}
		}

		private void append(int index, ConnectorProvider.Route connector, ConnectorProvider.Route padding)
				throws IOException {
			String connectorString = "";
			if (index < this.printDownline.size() && this.printDownline.getInt(index) > 0) {
				if (connector != null)
					connectorString = GraphPrinter.this.connectorProvider.getConnectorString(connector,
						ConnectorProvider.Route.TOP_DOWN);
				else
					connectorString = GraphPrinter.this.connectorProvider
						.getConnectorString(ConnectorProvider.Route.TOP_DOWN);
			} else if (connector != null)
				connectorString = GraphPrinter.this.connectorProvider.getConnectorString(connector);

			String paddedString = String.format(GraphPrinter.this.widthString, connectorString);
			if (padding != null)
				paddedString = paddedString.replaceAll(" ",
					GraphPrinter.this.connectorProvider.getConnectorString(padding));
			this.appender.append(paddedString);
		}

		private void increaseDownline(int index, int count) {
			while (this.printDownline.size() < index + 1)
				this.printDownline.add(0);
			this.printDownline.set(index, this.printDownline.getInt(index) + count);
		}

		private void printConnection(int sourceIndex, int targetIndex) throws IOException {
			int startIndex = Math.min(sourceIndex, targetIndex);
			for (int index = 0; index < startIndex; index++)
				this.append(index, null, null);

			if (sourceIndex != -1)
				if (sourceIndex < targetIndex) {
					this.append(startIndex, ConnectorProvider.Route.TOP_RIGHT,
						ConnectorProvider.Route.LEFT_RIGHT);
					for (int index = sourceIndex + 1; index < targetIndex; index++)
						this.append(index, null, ConnectorProvider.Route.LEFT_RIGHT);
				} else if (sourceIndex == targetIndex)
					this.append(startIndex, ConnectorProvider.Route.TOP_DOWN, null);
				else {
					this.append(startIndex, ConnectorProvider.Route.RIGHT_DOWN,
						ConnectorProvider.Route.RIGHT_LEFT);
					for (int index = targetIndex + 1; index < sourceIndex; index++)
						this.append(index, null, ConnectorProvider.Route.RIGHT_LEFT);
				}

			int endIndex = Math.max(sourceIndex, targetIndex);
			if (sourceIndex < targetIndex)
				this.append(endIndex, ConnectorProvider.Route.LEFT_DOWN, null);
			else if (sourceIndex > targetIndex)
				this.append(endIndex, ConnectorProvider.Route.TOP_LEFT, null);

			for (int index = endIndex + 1; index < this.printDownline.size(); index++)
				this.append(index, null, null);

			this.appender.append('\n');
		}

		private void printConnections(int levelIndex, Level<Object> level) throws IOException {
			if (levelIndex > 0) {
				boolean printedConnection = false;
				for (int sourceIndex = 0; sourceIndex < level.getLevelNodes().size(); sourceIndex++) {
					Object node = level.getLevelNodes().get(sourceIndex);

					List<Object> inputs = level.getLinks(node);
					this.increaseDownline(sourceIndex, -1);
					for (int index = 0; index < inputs.size(); index++) {
						int targetIndex = this.levels.get(levelIndex - 1).getLevelNodes().indexOf(inputs.get(index));
						this.printConnection(sourceIndex, targetIndex);
						this.increaseDownline(targetIndex, 1);
						printedConnection = true;
					}
				}
				if (!printedConnection)
					this.printConnection(-1, -1);
			}
		}

		private void printDAG() throws IOException {
			for (int levelIndex = this.levels.size() - 1; levelIndex >= 0; levelIndex--) {
				Level<Object> level = this.levels.get(levelIndex);
				for (int sourceIndex = 0; sourceIndex < level.getLevelNodes().size(); sourceIndex++) {
					Object node = level.getLevelNodes().get(sourceIndex);
					if (levelIndex == this.levels.size() - 1)
						this.increaseDownline(sourceIndex, level.getLinks(node).size());
					this.printNode(node);
				}
				this.appender.append('\n');

				this.printConnections(levelIndex, level);
			}
		}

		@SuppressWarnings("unchecked")
		private void printNode(Object node) throws IOException {
			if (node instanceof Placeholder)
				this.appender.append(String.format(GraphPrinter.this.widthString,
					((Placeholder) node).toString(GraphPrinter.this.connectorProvider)));
			else {
				String nodeString = GraphPrinter.this.nodePrinter.toString((Node) node);
				if (nodeString.length() > GraphPrinter.this.width)
					nodeString = nodeString.substring(0, GraphPrinter.this.width);
				this.appender.append(String.format(GraphPrinter.this.widthString, nodeString));
			}
		}

	}

	/**
	 * Default printer, which simply invokes {@link Object#toString()}
	 * 
	 * @author Arvid Heise
	 * @param <Node>
	 *        the class of the node
	 */
	public static class StandardPrinter<Node> implements NodePrinter<Node> {
		@Override
		public String toString(Object node) {
			return node.toString();
		}

	}
}
