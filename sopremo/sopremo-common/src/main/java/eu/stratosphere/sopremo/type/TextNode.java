package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Formatter;
import java.util.Locale;

import eu.stratosphere.pact.common.type.base.PactString;

/**
 * This node represents a string value.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class TextNode extends AbstractJsonNode implements IPrimitiveNode, CharSequence, Appendable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663376747000392562L;

	public final static TextNode EMPTY_STRING_NODE = new TextNode("");

	private transient PactString value;

	private transient Formatter formatter;

	/**
	 * Initializes a TextNode which represents an empty String.
	 */
	public TextNode() {
		this.value = new PactString();
	}

	/**
	 * Initializes a TextNode which represents the given <code>String</code>. To create new TextNodes please
	 * use TextNode.valueOf(<code>String</code>) instead.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 */
	public TextNode(final CharSequence v) {
		this.value = new PactString(v);
	}

	@Override
	public CharSequence getJavaValue() {
		return this.value.getValue();
	}

	public Formatter asFormatter() {
		if (this.formatter == null)
			this.formatter = new Formatter(this, Locale.US);
		return this.formatter;
	}

	/**
	 * Creates a new instance of TextNode. This new instance represents the given value.
	 * 
	 * @param v
	 *        the value that should be represented by the new instance
	 * @return the newly created instance of TextNode
	 */
	public static TextNode valueOf(final String v) {
		if (v == null)
			throw new NullPointerException();
		if (v.length() == 0)
			return EMPTY_STRING_NODE;
		return new TextNode(v);
	}

	/**
	 * Returns the String which is represented by this node.
	 * 
	 * @return the represented String
	 */
	public CharSequence getTextValue() {
		return this.getJavaValue();
	}

	public void setValue(final String value) {
		this.value.setValue(value);
	}

	@Override
	public void toString(final StringBuilder sb) {
		appendQuoted(sb, this.value.toString());
	}

	/**
	 * Returns the directly backing char array, which may be larger than the actual length.
	 * 
	 * @return the backing array
	 */
	public char[] asCharArray() {
		return this.value.getCharArray();
	}

	/**
	 * Appends the given String with a leading and ending " to the given StringBuilder.
	 * 
	 * @param sb
	 *        the StringBuilder where the quoted String should be added to
	 * @param content
	 *        the String that should be appended
	 */
	public static void appendQuoted(final StringBuilder sb, final String content) {
		sb.append('"');
		sb.append(content);
		sb.append('"');
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
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
		return this.value.equals(other.value);
	}

	@Override
	public void clear() {
		this.value.setLength(0);
	}

	public void setLength(int len) {
		this.value.setLength(len);
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
	public int compareToSameType(final IJsonNode other) {
		return this.value.compareTo(((TextNode) other).value);
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeUTF(this.value.getValue());
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		this.value = new PactString(in.readUTF());
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.checkForSameType(otherNode);
		this.value.setValue(((TextNode) otherNode).value);
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return this.value.getMaxNormalizedKeyLen();
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		this.value.copyNormalizedKey(target, offset, len);
	}

	public void setValue(TextNode text, int start, int end) {
		this.value.setValue(text.value, start, end - start);
	}

	public int find(CharSequence str) {
		return this.value.find(str);
	}

	public int find(CharSequence str, int start) {
		return this.value.find(str, start);
	}

	@Override
	public int length() {
		return this.value.length();
	}

	@Override
	public char charAt(int index) {
		return this.value.charAt(index);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return this.value.subSequence(start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence)
	 */
	@Override
	public Appendable append(CharSequence csq) {
		this.value.append(csq);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence, int, int)
	 */
	@Override
	public Appendable append(CharSequence csq, int start, int end) {
		this.value.append(csq, start, end);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence)
	 */
	public Appendable append(TextNode csq) {
		this.value.append(csq.value);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence, int, int)
	 */
	public Appendable append(TextNode csq, int start, int end) {
		this.value.append(csq.value, start, end);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(char)
	 */
	@Override
	public Appendable append(char c) {
		this.value.append(c);
		return this;
	}

	/**
	 * @param incrementAndGet
	 */
	public void append(long number) {
		this.formatter.format("%d", number);
	}

}
