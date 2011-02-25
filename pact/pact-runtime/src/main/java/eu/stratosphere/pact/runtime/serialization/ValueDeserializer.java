package eu.stratosphere.pact.runtime.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.type.Value;

/**
 * Deserializer for {@link Value} objects. 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <V>
 */
public class ValueDeserializer<V extends Value> implements RecordDeserializer<Value> {

	private ClassLoader classLoader;
	private Class<Value> valueClass;

	public ValueDeserializer() {
	}

	public ValueDeserializer(Class<Value> valueClass) {
		this.valueClass = valueClass;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#deserialize(java.io.DataInput)
	 */
	@Override
	public Value deserialize(DataInput in) {
		try {
			Value value = getInstance();
			value.read(in);
			return value;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#getRecordType()
	 */
	@Override
	public Class<Value> getRecordType() {
		return valueClass;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {
		try {
			this.valueClass = (Class<Value>) Class.forName(StringRecord.readString(in), true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		StringRecord.writeString(out, this.valueClass.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#setClassLoader(java.lang.ClassLoader)
	 */
	@Override
	public void setClassLoader(ClassLoader classLoader) {

		this.classLoader = classLoader;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#getInstance()
	 */
	@Override
	public Value getInstance() {
		try {
			return valueClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
