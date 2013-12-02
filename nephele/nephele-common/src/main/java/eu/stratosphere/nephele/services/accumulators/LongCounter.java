package eu.stratosphere.nephele.services.accumulators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class LongCounter implements SimpleAccumulator<Long> {
  
  private static final long serialVersionUID = 1L;

  private long localValue = 0;
  
  @Override
  public void add(Long value) {
    this.localValue += value;
  }

  @Override
  public Long getLocalValue() {
    return this.localValue;
  }

  @Override
  public void merge(Accumulator<Long, Long> other) {
  	this.localValue += ((LongCounter)other).getLocalValue();
  }

  @Override
  public void resetLocal() {
    this.localValue = 0;
  }

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.localValue);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.localValue = in.readLong();
	}
	
	@Override
	public String toString() {
		return "LongCounter object. Local value: " + this.localValue;
	}

}
