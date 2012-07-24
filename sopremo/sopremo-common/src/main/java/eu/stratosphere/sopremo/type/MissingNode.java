package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * This node represents a missing value.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class MissingNode extends AbstractJsonNode implements IPrimitiveNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5057162510515824922L;

	private final static MissingNode Instance = new MissingNode();

	/**
	 * Initializes a MissingNode. This constructor is needed for serialization and
	 * deserialization of MissingNodes, please use MissingNode.getInstance() to get the instance of MissingNode.
	 */
	public MissingNode() {
	}

	/**
	 * Returns the instance of MissingNode.
	 * 
	 * @return the instance of MissingNode
	 */
	public static MissingNode getInstance() {
		return Instance;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append("<missing>");
	}

	@Override
	public boolean equals(final Object o) {
		return this == o;
		// throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public MissingNode canonicalize() {
		return Instance;
	}

	@Override
	public void read(final DataInput in) throws IOException {
	}

	@Override
	public void write(final DataOutput out) throws IOException {
	}

	@Override
	public boolean isNull() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonNode#isMissing()
	 */
	@Override
	public boolean isMissing() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.MissingNode;
	}

	private Object readResolve() {
		return Instance;
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
		return 0;
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.checkForSameType(otherNode);
	}

	@SuppressWarnings("unused")
	private void writeObject(final ObjectOutputStream oos) throws IOException {
	}

	@SuppressWarnings("unused")
	private void readObject(final ObjectInputStream in) throws IOException {
	}

	@Override
	public int hashCode() {
		return 42;
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("MissingNode");
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return 0;
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		this.fillWithZero(target, offset, offset + len);
	}

}
