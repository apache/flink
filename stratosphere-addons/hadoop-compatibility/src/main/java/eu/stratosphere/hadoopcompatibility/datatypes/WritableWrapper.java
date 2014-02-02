package eu.stratosphere.hadoopcompatibility.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;

public class WritableWrapper<T extends Writable> implements Value {
	private static final long serialVersionUID = 2L;
	
	private T wrapped;
	private String wrappedType;
	private ClassLoader cl;
	
	public WritableWrapper() {
	}
	
	public WritableWrapper(T toWrap) {
		wrapped = toWrap;
		wrappedType = toWrap.getClass().getCanonicalName();
	}

	public <X extends Writable> X value() {
		return (X) wrapped;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(wrappedType);
		wrapped.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		if(cl == null) {
			cl = Thread.currentThread().getContextClassLoader();
		}
		wrappedType = in.readUTF();
		try {
			Class wrClass = Class.forName(wrappedType, true, cl);
			wrapped = (T) InstantiationUtil.instantiate(wrClass, Writable.class);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error creating the WritableWrapper", e);
		}
		wrapped.readFields(in);
	}

}
