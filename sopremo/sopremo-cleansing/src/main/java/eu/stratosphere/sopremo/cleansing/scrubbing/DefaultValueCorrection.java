package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.pact.SopremoUtil;

public class DefaultValueCorrection extends ValueCorrection {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1536110850287975405L;

	/**
	 * The default, stateless instance.
	 */
	public final static DefaultValueCorrection NULL = new DefaultValueCorrection(NullNode.getInstance());

	private transient JsonNode defaultValue;

	public DefaultValueCorrection(final JsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.defaultValue = SopremoUtil.deserializeNode(ois, JsonNode.class);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		SopremoUtil.serializeNode(oos, defaultValue);
	}

	@Override
	public JsonNode fix(final JsonNode value, final ValidationContext context) {
		return this.defaultValue;
	}

}
