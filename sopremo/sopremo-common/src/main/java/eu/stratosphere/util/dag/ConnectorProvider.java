package eu.stratosphere.util.dag;

/**
 * Returns printable characters for all different kinds of edge intersections.
 * 
 * @see GraphPrinter
 * @author Arvid Heise
 */
public interface ConnectorProvider {
	/**
	 * Returns the string representing all {@link Route}s. The actual string depends on the implementation and may or
	 * may not account for intersections or several occurrences of the same route.
	 * 
	 * @param routes
	 *        the routes that should be represented
	 * @return a string representation.
	 */
	public String getConnectorString(Route... routes);

	/**
	 * The basic four direction used to compose the complex {@link Route}.
	 * 
	 * @author Arvid Heise
	 */
	static enum BaseDirection {
		TOP, DOWN, RIGHT, LEFT
	};

	/**
	 * Represents a line from a starting point to an ending point at a given square.
	 * 
	 * @author Arvid Heise
	 */
	static enum Route {
		TOP_DOWN(BaseDirection.TOP, BaseDirection.DOWN),
		TOP_RIGHT(BaseDirection.TOP, BaseDirection.RIGHT),
		TOP_LEFT(BaseDirection.TOP, BaseDirection.LEFT),
		LEFT_DOWN(BaseDirection.LEFT, BaseDirection.DOWN),
		RIGHT_DOWN(BaseDirection.RIGHT, BaseDirection.DOWN),
		LEFT_RIGHT(BaseDirection.LEFT, BaseDirection.RIGHT),
		RIGHT_LEFT(BaseDirection.RIGHT, BaseDirection.LEFT);

		private final BaseDirection from, to;

		private Route(final BaseDirection from, final BaseDirection to) {
			this.from = from;
			this.to = to;
		}

		public BaseDirection getFrom() {
			return this.from;
		}

		public BaseDirection getTo() {
			return this.to;
		}
	}
}