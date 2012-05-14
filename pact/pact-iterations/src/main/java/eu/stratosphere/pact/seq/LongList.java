package eu.stratosphere.pact.seq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class LongList implements Value{

	private long[] numbers = new long[0];
	
	private int count = 0;
	
	public LongList() {
		
	}
	
	public void clear() {
		count = 0;
	}
	
	public void add(long number) {
		ensureSize(count + 1);
		numbers[count] = number;
		count++;
	}
	
	private void ensureSize(int size) {
		if(numbers.length < size) {
			int newSize = numbers.length;
			while(newSize < size) {
				newSize *= 2;
			}
			
			long[] newApexes = new long[newSize];
			System.arraycopy(numbers, 0, newApexes, 0, numbers.length);
			numbers = newApexes;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		for (int i = 0; i < count; i++) {
			out.writeLong(numbers[i]);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		count = in.readInt();
		ensureSize(count);
		for (int i = 0; i < count; i++) {
			numbers[i] = in.readLong();
		}
	}

	public long[] getNumbers() {
		long[] target = new long[count];
		System.arraycopy(numbers, 0, target, 0, count);
		return target;
	}
}
