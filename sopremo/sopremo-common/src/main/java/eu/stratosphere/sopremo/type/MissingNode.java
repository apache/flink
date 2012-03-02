package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class MissingNode extends JsonNode implements IPrimitiveNode{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5057162510515824922L;

	private final static MissingNode Instance = new MissingNode();

	public MissingNode() {

	}

	public static MissingNode getInstance() {
		return Instance;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append("<missing>");
	}

	@Override
	public boolean equals(final Object o) {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public MissingNode canonicalize() {
		return Instance;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public boolean isNull() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonNode#isMissing()
	 */
	@Override
	public boolean isMissing() {
		return true;
	}

	@Override
	public Type getType() {
		throw new UnsupportedOperationException("MissingNode");
	}

	private Object readResolve() {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public IJsonNode clone() {
		return this;
	}

	@Override
	public Object getJavaValue() {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		throw new UnsupportedOperationException("MissingNode");
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		throw new UnsupportedOperationException("MissingNode");
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public int hashCode() {
		return 42;
	}
}
