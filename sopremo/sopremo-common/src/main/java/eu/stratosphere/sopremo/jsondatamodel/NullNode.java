package eu.stratosphere.sopremo.jsondatamodel;

public class NullNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5057162510515824922L;

	private final static NullNode instance = new NullNode();

	private NullNode() {
	};

	public static NullNode getInstance() {
		return instance;
	}

	@Override
	public String toString() {
		return "null";
	}

	@Override
	public boolean equals(final Object o) {
		return o == this;
	}
}
