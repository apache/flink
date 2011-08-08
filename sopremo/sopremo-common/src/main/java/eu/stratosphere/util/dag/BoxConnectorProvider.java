package eu.stratosphere.util.dag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BoxConnectorProvider implements ConnectorProvider {
	private final Map<List<BaseDirection>, String> connectorStrings = new HashMap<List<BaseDirection>, String>();

	private final Comparator<Enum<?>> EnumComparator = new Comparator<Enum<?>>() {
		@Override
		public int compare(final Enum<?> o1, final Enum<?> o2) {
			return o1.ordinal() - o2.ordinal();
		}
	};

	public BoxConnectorProvider() {
		this.put(new ArrayList<BaseDirection>(), "");

		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.DOWN), "\u2502");
		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.RIGHT), "\u2514");
		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.LEFT), "\u2518");
		this.put(Arrays.asList(BaseDirection.RIGHT, BaseDirection.DOWN), "\u250C");
		this.put(Arrays.asList(BaseDirection.LEFT, BaseDirection.DOWN), "\u2510");
		this.put(Arrays.asList(BaseDirection.LEFT, BaseDirection.RIGHT), "\u2500");

		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.TOP, BaseDirection.LEFT, BaseDirection.DOWN),
			"\u2526");
		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.TOP, BaseDirection.RIGHT, BaseDirection.DOWN),
			"\u251E");
		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.RIGHT, BaseDirection.DOWN, BaseDirection.DOWN),
			"\u251F");
		this.put(Arrays.asList(BaseDirection.TOP, BaseDirection.LEFT, BaseDirection.DOWN, BaseDirection.DOWN),
			"\u2527");
	}

	@Override
	public String getConnectorString(final Route... connectors) {
		final List<BaseDirection> directionList = new ArrayList<BaseDirection>();

		for (final Route connector : connectors) {
			directionList.add(connector.getFrom());
			directionList.add(connector.getTo());
		}
		Collections.sort(directionList, this.EnumComparator);

		return this.connectorStrings.get(directionList);
	}

	private void put(final List<BaseDirection> list, final String string) {
		Collections.sort(list, this.EnumComparator);
		this.connectorStrings.put(list, string);
	};
}