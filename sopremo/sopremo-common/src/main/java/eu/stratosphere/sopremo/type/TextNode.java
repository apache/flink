package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eu.stratosphere.pact.common.type.base.PactString;

public class TextNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663376747000392562L;

	final static TextNode EMPTY_STRING_NODE = new TextNode("");

	protected transient PactString value;

	public TextNode() {
		this.value = new PactString();
	}

	public TextNode(final String v) {
		this.value = new PactString(v);
	}
	
	@Override
	public Object getJavaValue() {
		return this.value.getValue();
	}

	public static TextNode valueOf(final String v) {
		if (v == null)
			throw new NullPointerException();
		if (v.length() == 0)
			return EMPTY_STRING_NODE;
		return new TextNode(v);
	}

	public String getTextValue() {
		return this.value.toString();
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		appendQuoted(sb, this.value.toString());
		return sb;
	}

	public static void appendQuoted(final StringBuilder sb, final String content) {
		sb.append('"');
		sb.append(content);
		sb.append('"');
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.value.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final TextNode other = (TextNode) obj;
		if (!this.value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.value.read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		this.value.write(out);
	}

	@Override
	public boolean isTextual() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.TextNode;
	}

	@Override
	public int compareToSameType(final JsonNode other) {
		return this.value.compareTo(((TextNode) other).value);
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeUTF(this.value.getValue());
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		this.value = new PactString(in.readUTF());
	}

	@Override
	public TextNode clone() {
		final TextNode clone = (TextNode) super.clone();
		clone.value = new PactString(this.value.getValue());
		return clone;
	}

}
