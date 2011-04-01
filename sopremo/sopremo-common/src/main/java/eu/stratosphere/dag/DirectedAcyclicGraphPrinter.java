package eu.stratosphere.dag;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utility class to pretty print arbitrary directed acyclic graphs. It needs a {@link Navigator} to traverse form the
 * start nodes through the graph and optionally a {@link NodePrinter} to format the nodes.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the node
 */
public class DirectedAcyclicGraphPrinter<Node> {
	private Collection<Node> nodes;

	private Navigator<Node> navigator;

	private ConnectorProvider connectorProvider = new BoxConnectorProvider();

	private class ASCIIConnectorProvider implements ConnectorProvider {
		@Override
		public String getConnectorString(Connector... connectors) {
			EnumSet<Connector> connectorSet = EnumSet.of(connectors[0], connectors);

			if (connectorSet.contains(Connector.TOP_LEFT))
				return "/";
			if (connectorSet.contains(Connector.TOP_RIGHT))
				return "\\";
			if (connectorSet.contains(Connector.RIGHT_DOWN))
				return "/";
			if (connectorSet.contains(Connector.LEFT_DOWN))
				return "\\";
			if (connectorSet.contains(Connector.TOP_DOWN))
				return "|";
			return "-";
		};
	}

	private class BoxConnectorProvider implements ConnectorProvider {
		private Map<List<Direction>, String> connectorStrings = new HashMap<List<Direction>, String>();

		public BoxConnectorProvider() {
			put(new ArrayList<Direction>(), "");

			put(Arrays.asList(Direction.TOP, Direction.DOWN), "\u2502");
			put(Arrays.asList(Direction.TOP, Direction.RIGHT), "\u2514");
			put(Arrays.asList(Direction.TOP, Direction.LEFT), "\u2518");
			put(Arrays.asList(Direction.RIGHT, Direction.DOWN), "\u250C");
			put(Arrays.asList(Direction.LEFT, Direction.DOWN), "\u2510");
			put(Arrays.asList(Direction.LEFT, Direction.RIGHT), "\u2500");

			put(Arrays.asList(Direction.TOP, Direction.TOP, Direction.LEFT, Direction.DOWN), "\u2526");
			put(Arrays.asList(Direction.TOP, Direction.TOP, Direction.RIGHT, Direction.DOWN), "\u251E");
			put(Arrays.asList(Direction.TOP, Direction.RIGHT, Direction.DOWN, Direction.DOWN),
				"\u251F");
			put(Arrays.asList(Direction.TOP, Direction.LEFT, Direction.DOWN, Direction.DOWN), "\u2527");
		}

		private void put(List<Direction> list, String string) {
			Collections.sort(list, EnumComparator);
			connectorStrings.put(list, string);
		}

		private Comparator<Enum<?>> EnumComparator = new Comparator<Enum<?>>() {
			@Override
			public int compare(Enum<?> o1, Enum<?> o2) {
				return o1.ordinal() - o2.ordinal();
			}
		};

		@Override
		public String getConnectorString(Connector... connectors) {
			List<Direction> directionList = new ArrayList<Direction>();

			for (Connector connector : connectors) {
				directionList.add(connector.getFrom());
				directionList.add(connector.getTo());
			}
			Collections.sort(directionList, EnumComparator);

			return connectorStrings.get(directionList);
		};
	}

	private static interface ConnectorProvider {
		static enum Direction {
			TOP, DOWN, RIGHT, LEFT
		};

		static enum Connector {
			TOP_DOWN(Direction.TOP, Direction.DOWN), TOP_RIGHT(Direction.TOP, Direction.RIGHT), TOP_LEFT(Direction.TOP,
					Direction.LEFT), LEFT_DOWN(Direction.LEFT, Direction.DOWN), RIGHT_DOWN(Direction.RIGHT,
					Direction.DOWN), LEFT_RIGHT(Direction.LEFT, Direction.RIGHT), RIGHT_LEFT(Direction.RIGHT,
					Direction.LEFT);

			private Direction from, to;

			private Connector(Direction from, Direction to) {
				this.from = from;
				this.to = to;
			}

			public Direction getFrom() {
				return from;
			}

			public Direction getTo() {
				return to;
			}
		};

		public String getConnectorString(Connector... connectors);
	}

	/**
	 * Initializes DirectedAcyclicGraphPrinter with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public DirectedAcyclicGraphPrinter(Navigator<Node> navigator, Collection<Node> startNodes) {
		this.navigator = navigator;
		this.nodes = startNodes;
	}

	/**
	 * Initializes DirectedAcyclicGraphPrinter with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public DirectedAcyclicGraphPrinter(Navigator<Node> navigator, Iterable<Node> startNodes) {
		this(navigator, startNodes.iterator());
	}

	/**
	 * Initializes DirectedAcyclicGraphPrinter with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public DirectedAcyclicGraphPrinter(Navigator<Node> navigator, Iterator<Node> startNodes) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		while (startNodes.hasNext())
			this.nodes.add(startNodes.next());
	}

	/**
	 * Initializes DirectedAcyclicGraphPrinter with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public DirectedAcyclicGraphPrinter(Navigator<Node> navigator, Node... startNodes) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		for (Node node : startNodes)
			this.nodes.add(node);
	}

	/**
	 * Initializes DirectedAcyclicGraphPrinter with the given {@link Navigator} and all nodes reachable from the root
	 * node.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param rootNode
	 *        the root node
	 */
	public DirectedAcyclicGraphPrinter(Navigator<Node> navigator, Node rootNode) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		gatherNodes(rootNode);
	}

	private void gatherNodes(Node node) {
		this.nodes.add(node);
		for (Node child : navigator.getConnectedNodes(node))
			gatherNodes(child);
	}

	/**
	 * May provide an alternative implementation of the {@link Object#toString()}.
	 * 
	 * @author Arvid Heise
	 * @param <Node>
	 *        the class of the node
	 */
	public static interface NodePrinter<Node> {
		/**
		 * Returns a string representation of the given node.
		 * 
		 * @param node
		 *        the node
		 * @return a string representation.
		 * @see Object#toString()
		 */
		public String toString(Node node);
	}

	/**
	 * Place holder to format connections between nodes above several levels. There are two types: spacers and
	 * connectors.
	 * 
	 * @author Arvid Heise
	 */
	private static class Placeholder {
		private List<Object> targets = new ArrayList<Object>(1);

		private int index;

		/**
		 * Initializes a connector place holder to the given target.
		 * 
		 * @param target
		 *        the target
		 */
		public Placeholder(Object target, int index) {
			this.targets.add(target);
			this.index = index;
		}

		/**
		 * Initializes a spacer.
		 */
		public Placeholder() {
		}

		public String toString(ConnectorProvider connectorProvider) {
			if (this.targets.isEmpty())
				return "";
			return connectorProvider.getConnectorString(ConnectorProvider.Connector.TOP_DOWN);
			// return this.targets.isEmpty() ? "" : "|";
			// return this.targets.isEmpty() ? "" : String.valueOf(index) + targets.toString();
		}
	}

	private class Level {
		private IdentityHashMap<Object, List<Object>> outgoings = new IdentityHashMap<Object, List<Object>>();

		private List<Object> levelNodes = new ArrayList<Object>();

		public Level(List<Node> nodes) {
			this.levelNodes.addAll(nodes);

			// initializes all outgoing links
			for (Node node : nodes) {
				ArrayList<Object> links = new ArrayList<Object>();
				for (Object connectedNode : DirectedAcyclicGraphPrinter.this.navigator.getConnectedNodes(node))
					links.add(connectedNode);
				this.outgoings.put(node, links);
			}
		}

		public List<Object> getLinks(Object node) {
			return this.outgoings.get(node);
		}

		public List<Object> getLevelNodes() {
			return levelNodes;
		}

		@Override
		public String toString() {
			return this.levelNodes.toString();
		}

		/**
		 * Adds a connector place holder to the given object. Adds spacers if needed.
		 * 
		 * @param placeholderIndex
		 *        the index of the place holder in list of levelNodes
		 * @param to
		 *        the object to link to
		 * @return the place holder
		 */
		public Placeholder addPlaceholder(int placeholderIndex, Object to) {
			for (int index = this.levelNodes.size(); index < placeholderIndex; index++) {
				Placeholder emptyPlaceholder = new Placeholder();
				this.levelNodes.add(emptyPlaceholder);
				this.outgoings.put(emptyPlaceholder, new ArrayList<Object>());
			}

			for (; placeholderIndex < this.levelNodes.size(); placeholderIndex++) {
				if (!(this.levelNodes.get(placeholderIndex) instanceof Placeholder))
					break;
				if (((Placeholder) this.levelNodes.get(placeholderIndex)).targets.isEmpty()) {
					this.levelNodes.remove(placeholderIndex);
					break;
				}
			}

			Placeholder placeholder = new Placeholder(to, placeholderIndex);
			this.levelNodes.add(placeholderIndex, placeholder);
			this.outgoings.put(placeholder, new ArrayList<Object>(Arrays.asList(to)));
			return placeholder;
		}

		public int indexOf(Object input) {
			int index = 0;
			for (Object node : getLevelNodes()) {
				if (node == input)
					return index;
				index++;
			}
			return -1;
		}

		public void updateLink(Object from, Object to, Object placeholder) {
			this.outgoings.get(from).set(this.outgoings.get(from).indexOf(to), placeholder);
		}
	}

	private void addPlaceholders(List<Level> levels) {
		for (int levelIndex = levels.size() - 1; levelIndex >= 0; levelIndex--) {
			Level level = levels.get(levelIndex);
			List<Object> levelNodes = level.getLevelNodes();

			int placeHolderIndex = 0;
			for (int nodeIndex = 0; nodeIndex < levelNodes.size(); nodeIndex++) {
				Object node = levelNodes.get(nodeIndex);

				List<Object> inputs = level.getLinks(node);

				for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++, placeHolderIndex++) {
					Object input = inputs.get(inputIndex);
					if (levels.get(levelIndex - 1).indexOf(input) == -1) {
						Object placeholder = levels.get(levelIndex - 1).addPlaceholder(placeHolderIndex, input);
						level.updateLink(node, input, placeholder);
					}
				}

				// for (Object input : inputs) {
				// int targetIndex;
				// int targetLevelIndex = levelIndex;
				// do
				// targetIndex = levels.get(--targetLevelIndex).indexOf(input);
				// while (targetIndex == -1);
				// Object placeholder = input;
				// while (++targetLevelIndex < levelIndex)
				// placeholder = levels.get(targetLevelIndex).addPlaceholder(targetIndex, placeholder);
				// if (placeholder != input)
				// level.updateLink(node, input, placeholder);
				// }

			}
		}
	}

	private List<Level> getLevels() {
		Collection<Node> remainingNodes = new ArrayList<Node>(this.nodes);
		List<Node> usedNodes = new ArrayList<Node>();
		List<Level> levels = new ArrayList<Level>();

		while (!remainingNodes.isEmpty()) {
			List<Node> independentNodes = new ArrayList<Node>();

			for (Node node : remainingNodes)
				if (this.isIndependent(node, usedNodes))
					independentNodes.add(node);

			if (independentNodes.isEmpty())
				throw new IllegalStateException("graph does not have nodes without input");
			levels.add(new Level(new ArrayList<Node>(independentNodes)));

			remainingNodes.removeAll(independentNodes);
			usedNodes.addAll(independentNodes);
		}

		return levels;
	}

	private boolean isIndependent(Node node, Collection<Node> usedNodes) {
		for (Object input : this.navigator.getConnectedNodes(node))
			if (!usedNodes.contains(input))
				return false;
		return true;
	}

	private class Printer {
		private Appendable appender;

		private NodePrinter<Node> nodePrinter;

		private String widthString;

		private List<Level> levels;

		private IntList printDownline = new IntArrayList();

		private int width;

		private Printer(Appendable builder, NodePrinter<Node> nodePrinter, int width) {
			this.appender = builder;
			this.nodePrinter = nodePrinter;
			this.widthString = "%-" + width + "s";
			this.width = width;

			this.levels = DirectedAcyclicGraphPrinter.this.getLevels();
			DirectedAcyclicGraphPrinter.this.addPlaceholders(this.levels);
		}

		private void printDAG() throws IOException {
			for (int levelIndex = this.levels.size() - 1; levelIndex >= 0; levelIndex--) {
				Level level = this.levels.get(levelIndex);
				for (int sourceIndex = 0; sourceIndex < level.levelNodes.size(); sourceIndex++) {
					Object node = level.levelNodes.get(sourceIndex);
					increaseDownline(sourceIndex, level.getLinks(node).size());
					this.printNode(node);
				}
				this.appender.append("\n");

				printConnections(levelIndex, level);
			}
		}

		private void increaseDownline(int index, int count) {
			while (printDownline.size() < index + 1)
				printDownline.add(0);
			printDownline.set(index, printDownline.getInt(index) + count);
		}

		private void printConnections(int levelIndex, Level level) throws IOException {
			if (levelIndex > 0) {
				boolean printedConnection = false;
				for (int sourceIndex = 0; sourceIndex < level.levelNodes.size(); sourceIndex++) {
					Object node = level.levelNodes.get(sourceIndex);

					List<Object> inputs = level.getLinks(node);

					for (int index = inputs.size() - 1; index >= 0; index--) {
						int targetIndex = this.levels.get(levelIndex - 1).levelNodes.indexOf(inputs.get(index));

						if (sourceIndex == targetIndex)
							continue;

						this.printConnection(sourceIndex, targetIndex);
						printedConnection = true;

						increaseDownline(sourceIndex, -1);
						increaseDownline(targetIndex, 1);
					}
				}
				if (!printedConnection)
					this.printConnection(-1, -1);
			}
		}

		private void append(int index, ConnectorProvider.Connector connector, ConnectorProvider.Connector padding)
				throws IOException {
			String connectorString = "";
			if (index < printDownline.size() && printDownline.getInt(index) > 0) {
				if (connector != null)
					connectorString = connectorProvider.getConnectorString(connector,
						ConnectorProvider.Connector.TOP_DOWN);
				else
					connectorString = connectorProvider.getConnectorString(ConnectorProvider.Connector.TOP_DOWN);
			} else if (connector != null)
				connectorString = connectorProvider.getConnectorString(connector);

			String paddedString = String.format(this.widthString, connectorString);
			if (padding != null)
				paddedString = paddedString.replaceAll(" ", connectorProvider.getConnectorString(padding));
			this.appender.append(paddedString);
		}

		private void printConnection(int sourceIndex, int targetIndex) throws IOException {
			int startIndex = Math.min(sourceIndex, targetIndex);
			for (int index = 0; index < startIndex; index++)
				append(index, null, null);

			if (sourceIndex != -1) {
				if (sourceIndex < targetIndex) {
					append(startIndex, ConnectorProvider.Connector.TOP_RIGHT, ConnectorProvider.Connector.LEFT_RIGHT);
					for (int index = sourceIndex + 1; index < targetIndex; index++)
						append(index, null, ConnectorProvider.Connector.LEFT_RIGHT);
				} else if (sourceIndex == targetIndex)
					append(startIndex, null, null);
				else {
					append(startIndex, ConnectorProvider.Connector.RIGHT_DOWN, ConnectorProvider.Connector.RIGHT_LEFT);
					for (int index = targetIndex + 1; index < sourceIndex; index++)
						append(index, null, ConnectorProvider.Connector.RIGHT_LEFT);
				}
			}

			int endIndex = Math.max(sourceIndex, targetIndex);
			if (sourceIndex < targetIndex)
				append(endIndex, ConnectorProvider.Connector.LEFT_DOWN, null);
			else if (sourceIndex > targetIndex)
				append(endIndex, ConnectorProvider.Connector.TOP_LEFT, null);

			for (int index = endIndex + 1; index < printDownline.size(); index++)
				append(index, null, null);

			this.appender.append("\n");
		}

		@SuppressWarnings("unchecked")
		private void printNode(Object node) throws IOException {
			if (node instanceof Placeholder)
				this.appender.append(String.format(this.widthString, ((Placeholder) node).toString(connectorProvider)));
			else {
				String nodeString = this.nodePrinter.toString((Node) node);
				if (nodeString.length() > width)
					nodeString = nodeString.substring(0, width);
				this.appender.append(String.format(this.widthString, nodeString));
			}
		}

	}

	/**
	 * Converts the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link Formatter} format and all structural elements are formatted with the given width.
	 * 
	 * @param format
	 *        the format of the nodes
	 * @param width
	 *        the width of structural elements
	 * @return a string representation of the directed acyclic graph
	 */
	public String toString(final String format, int width) {
		return this.toString(new NodePrinter<Node>() {
			@Override
			public String toString(Node node) {
				return String.format(format, node.toString());
			}
		}, width);
	}

	/**
	 * Converts the directed acyclic graph to a string representation where each node is formatted using the
	 * {@link Object#toString()} method and all structural elements are formatted with the given width.
	 * 
	 * @param width
	 *        the width of structural elements
	 * @return a string representation of the directed acyclic graph
	 */
	public String toString(final int width) {
		return this.toString(new NodePrinter<Node>() {
			@Override
			public String toString(Node node) {
				return node.toString();
			}
		}, width);
	}

	/**
	 * Converts the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link NodePrinter} and all structural elements are formatted with the given width.
	 * 
	 * @param nodePrinter
	 *        the formatter of the nodes
	 * @param width
	 *        the width of structural elements
	 * @return a string representation of the directed acyclic graph
	 */
	public String toString(NodePrinter<Node> nodePrinter, int width) {
		StringBuilder builder = new StringBuilder();

		try {
			new Printer(builder, nodePrinter, width).printDAG();
		} catch (IOException e) {
		}

		return builder.toString();
	}

	/**
	 * Prints the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link Formatter} format and all structural elements are formatted with the given width.
	 * 
	 * @param appendable
	 *        the target of the print operations
	 * @param format
	 *        the format of the nodes
	 * @param width
	 *        the width of structural elements
	 * @throws IOException
	 *         if an I/O error occurred during the print operation
	 */
	public void print(Appendable appendable, final String format, int width) throws IOException {
		this.print(appendable, new NodePrinter<Node>() {
			@Override
			public String toString(Node node) {
				return String.format(format, node.toString());
			}
		}, width);
	}

	/**
	 * Prints the directed acyclic graph to a string representation where each node is formatted using the
	 * {@link Object#toString()} method and all structural elements are formatted with the given width.
	 * 
	 * @param appendable
	 *        the target of the print operations
	 * @param width
	 *        the width of structural elements
	 * @throws IOException
	 *         if an I/O error occurred during the print operation
	 */
	public void print(Appendable appendable, final int width) throws IOException {
		this.print(appendable, new NodePrinter<Node>() {
			String widthString = "%-" + width + "s";

			@Override
			public String toString(Node node) {
				return String.format(this.widthString, node.toString());
			}
		}, width);
	}

	/**
	 * Prints the directed acyclic graph to a string representation where each node is formatted by the specified
	 * {@link NodePrinter} and all structural elements are formatted with the given width.
	 * 
	 * @param appendable
	 *        the target of the print operations
	 * @param nodePrinter
	 *        the formatter of the nodes
	 * @param width
	 *        the width of structural elements
	 * @throws IOException
	 *         if an I/O error occurred during the print operation
	 */
	public void print(Appendable appendable, NodePrinter<Node> nodePrinter, int width) throws IOException {
		new Printer(appendable, nodePrinter, width).printDAG();
	}
}
