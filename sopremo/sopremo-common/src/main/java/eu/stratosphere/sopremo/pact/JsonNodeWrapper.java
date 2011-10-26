package eu.stratosphere.sopremo.pact;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class JsonNodeWrapper extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3195619585864989618L;

	private JsonNode value;

	public JsonNodeWrapper() {
	};

	/**
	 * Initializes JsonNodeWrapper.
	 * 
	 * @param value
	 */
	public JsonNodeWrapper(JsonNode value) {
		super();
		this.value = value;
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.value = SopremoUtil.deserializeNode(in);
		value.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		SopremoUtil.serializeNode(out, this.value);
		this.value.write(out);
	}

	public JsonNode getValue() {
		return this.value;
	}

	/**
	 * Sets the value to the specified value.
	 * 
	 * @param value
	 *        the value to set
	 */
	public void setValue(JsonNode value) {
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.value = value;
	}

	@Override
	public int compareToSameType(JsonNode other) {
		return this.value.compareTo(((JsonNodeWrapper) other).getValue());
	}

	@Override
	public boolean equals(Object o) {
		return this.value.equals(((JsonNodeWrapper) o).getValue());
	}

	@Override
	public int getTypePos() {
		return this.value.getTypePos();
	}

	@Override
	public TYPES getType() {
		return this.value.getType();
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		return this.value.toString(sb);
	}

}
