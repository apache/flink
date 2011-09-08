package eu.stratosphere.sopremo.jsondatamodel;

public class TextNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663376747000392562L;

	final static TextNode EMPTY_STRING_NODE = new TextNode("");

	protected String value;

	public TextNode(final String v) {
		this.value = v;
	}

	public static JsonNode valueOf(final String v) {
		if (v == null)
			return NullNode.getInstance();
		if (v.length() == 0)
			return EMPTY_STRING_NODE;
		return new TextNode(v);
	}

	public String getTextValue() {
		return this.value;
	}

	@Override
	public String toString() {
		int len = this.value.length();
		len += 2 + (len >> 4);
		final StringBuilder sb = new StringBuilder(len);
		appendQuoted(sb, this.value);
		return sb.toString();
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

}
