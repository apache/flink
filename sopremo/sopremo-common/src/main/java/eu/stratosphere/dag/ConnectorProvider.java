package eu.stratosphere.dag;

/**
 * Returns printable characters for all different kinds of edge intersections.
 * 
 * @see DAGPrinter
 * @author Arvid.Heise
 */
public interface ConnectorProvider {
	static enum Connector {
		TOP_DOWN(Direction.TOP, Direction.DOWN), TOP_RIGHT(Direction.TOP, Direction.RIGHT), TOP_LEFT(Direction.TOP,
				Direction.LEFT), LEFT_DOWN(Direction.LEFT, Direction.DOWN), RIGHT_DOWN(Direction.RIGHT,
				Direction.DOWN), LEFT_RIGHT(Direction.LEFT, Direction.RIGHT), RIGHT_LEFT(Direction.RIGHT,
				Direction.LEFT);

		private ConnectorProvider.Direction from, to;

		private Connector(ConnectorProvider.Direction from, ConnectorProvider.Direction to) {
			this.from = from;
			this.to = to;
		}

		public ConnectorProvider.Direction getFrom() {
			return from;
		}

		public ConnectorProvider.Direction getTo() {
			return to;
		}
	};

	static enum Direction {
		TOP, DOWN, RIGHT, LEFT
	};

	public String getConnectorString(ConnectorProvider.Connector... connectors);
}