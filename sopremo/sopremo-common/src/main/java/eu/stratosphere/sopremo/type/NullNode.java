package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eu.stratosphere.pact.common.type.base.PactNull;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class NullNode extends AbstractJsonNode implements IPrimitiveNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5057162510515824922L;

	private final static NullNode Instance = new NullNode();

	/**
	 * Initializes a NullNode. This constructor is needed for serialization and
	 * deserialization of NullNodes, please use NullNode.getInstance() to get the instance of NullNode.
	 */
	public NullNode() {
	}

	/**
	 * Returns the instance of NullNode.
	 * 
	 * @return the instance of NullNode
	 */
	public static NullNode getInstance() {
		return Instance;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		return sb.append("null");
	}

	@Override
	public boolean equals(final Object o) {
		return o == Instance;
//		return o instanceof NullNode ? true : false;
	}

	@Override
	public NullNode canonicalize() {
		return Instance;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		PactNull.getInstance().read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		PactNull.getInstance().write(out);
	}

	@Override
	public boolean isNull() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.NullNode;
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
		return null;
	}

	@Override
	public void copyValueFrom(IJsonNode otherNode) {
		this.checkForSameType(otherNode);
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return 0;
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeBoolean(false);
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		in.readBoolean();
	}

	@Override
	public int hashCode() {
		return 37;
	}

	@Override
	public void clear() {
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return PactNull.getInstance().getMaxNormalizedKeyLen();
	}

	@Override
	public void copyNormalizedKey(byte[] target, int offset, int len) {
		PactNull.getInstance().copyNormalizedKey(target, offset, len);
	}
}
