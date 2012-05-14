package eu.stratosphere.pact.programs.connected.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class PactBoolean implements Value {

	private boolean value;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(value);
	}

	@Override
	public void read(DataInput in) throws IOException {
		value = in.readBoolean();
	}

	public boolean isValue() {
		return value;
	}
	
	public void setValue(boolean value) {
		this.value = value;
	}
}
