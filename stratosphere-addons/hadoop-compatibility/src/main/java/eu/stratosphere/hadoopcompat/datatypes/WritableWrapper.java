package eu.stratosphere.hadoopcompat.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;

public class WritableWrapper implements Value {
	private static final long serialVersionUID = 1L;
	
	private Writable wrapped;
	private String wrappedType;
	
	public WritableWrapper() {
	}
	
	public WritableWrapper(Writable toWrap) {
		wrapped = toWrap;
		wrappedType = toWrap.getClass().getCanonicalName();
	}

	public Writable value() {
		return wrapped;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(wrappedType);
		wrapped.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		wrappedType = in.readUTF();
		try {
			Class wrClass = Class.forName(wrappedType);
			wrapped = InstantiationUtil.instantiate(wrClass, Writable.class);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error cerating the WritableWrapper");
		}
		
		wrapped.readFields(in);
	}

}
