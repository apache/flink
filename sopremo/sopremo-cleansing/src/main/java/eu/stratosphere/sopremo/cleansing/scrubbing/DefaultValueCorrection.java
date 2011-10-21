package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class DefaultValueCorrection extends ValueCorrection {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1536110850287975405L;

	/**
	 * The default, stateless instance.
	 */
	public final static DefaultValueCorrection NULL = new DefaultValueCorrection(NullNode.getInstance());

	private  JsonNode defaultValue;

	public DefaultValueCorrection(final JsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}

	

	@Override
	public JsonNode fix(final JsonNode value, final ValidationContext context) {
		return this.defaultValue;
	}

}
