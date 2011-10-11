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
		try {
			this.value = Type.values()[in.readInt()].getClazz().newInstance();
			this.value.read(in);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.value.getType().ordinal());
		this.value.write(out);
	}

	public JsonNode getValue() {
		return this.value;
	}

	@Override
	public int compareToSameType(JsonNode other) {
		return this.value.compareTo(((JsonNodeWrapper) other).getValue());
	}

	@Override
	public Object getJavaValue() {
		return this.value.getJavaValue();
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return this.value.equals(((JsonNodeWrapper) o).getValue());
	}

	@Override
	public Type getType() {
		return this.value.getType();
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		return this.value.toString(sb);
	}

}
