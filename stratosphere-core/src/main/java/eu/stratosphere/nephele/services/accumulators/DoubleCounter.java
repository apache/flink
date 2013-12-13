package eu.stratosphere.nephele.services.accumulators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleCounter implements SimpleAccumulator<Double> {

	private static final long serialVersionUID = 1L;

	private double localValue = 0;

	@Override
	public void add(Double value) {
		localValue += value;
	}

	@Override
	public Double getLocalValue() {
		return localValue;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		this.localValue += ((DoubleCounter) other).getLocalValue();
	}

	@Override
	public void resetLocal() {
		this.localValue = 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(localValue);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.localValue = in.readDouble();
	}

	@Override
	public String toString() {
		return "DoubleCounter object. Local value: " + this.localValue;
	}

}
