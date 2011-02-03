package eu.stratosphere.pact.runtime.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.type.Value;

/**
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

	
	private Class<Value> getValueClass()
	{
		return valueClass;
	}

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

	@Override
	public Class<Value> getRecordType() {
		return valueClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {
		try {
			this.valueClass = (Class<Value>) Class.forName(StringRecord.readString(in), true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringRecord.writeString(out, this.valueClass.getName());
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {

		this.classLoader = classLoader;
	}

	@Override
	public Value getInstance() {
		try {
			return valueClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
