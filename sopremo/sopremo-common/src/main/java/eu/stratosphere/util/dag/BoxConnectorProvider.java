package eu.stratosphere.util.dag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BoxConnectorProvider implements ConnectorProvider {
	private Map<List<BaseDirection>, String> connectorStrings = new HashMap<List<BaseDirection>, String>();

	private Comparator<Enum<?>> EnumComparator = new Comparator<Enum<?>>() {
		@Override
		public int compare(Enum<?> o1, Enum<?> o2) {
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
	public String getConnectorString(Route... connectors) {
		List<BaseDirection> directionList = new ArrayList<BaseDirection>();

		for (Route connector : connectors) {
			directionList.add(connector.getFrom());
			directionList.add(connector.getTo());
		}
		Collections.sort(directionList, this.EnumComparator);

		return this.connectorStrings.get(directionList);
	}

	private void put(List<BaseDirection> list, String string) {
		Collections.sort(list, this.EnumComparator);
		this.connectorStrings.put(list, string);
	};
}