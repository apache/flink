package eu.stratosphere.accumulators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Histogram for discrete-data. Let's you populate a histogram distributedly.
 * Implemented as a Integer->Integer HashMap
 * 
 * Could be extended to continuous values later, but then we need to dynamically
 * decide about the bin size in an online algorithm (or ask the user)
 */
public class Histogram implements Accumulator<Integer, Map<Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	private Map<Integer, Integer> hashMap = Maps.newHashMap();

	@Override
	public void add(Integer value) {
		Integer current = hashMap.get(value);
		Integer newValue = value;
		if (current != null) {
			newValue = current + newValue;
		}
		this.hashMap.put(value, newValue);
	}

	@Override
	public Map<Integer, Integer> getLocalValue() {
		return this.hashMap;
	}

	@Override
	public void merge(Accumulator<Integer, Map<Integer, Integer>> other) {
		// Merge the values into this map
		for (Map.Entry<Integer, Integer> entryFromOther : ((Histogram) other).getLocalValue()
				.entrySet()) {
			Integer ownValue = this.hashMap.get(entryFromOther.getKey());
			if (ownValue == null) {
				this.hashMap.put(entryFromOther.getKey(), entryFromOther.getValue());
			} else {
				this.hashMap.put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
			}
		}
	}

	@Override
	public void resetLocal() {
		this.hashMap.clear();
	}

	@Override
	public String toString() {
		return this.hashMap.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(hashMap.size());
		for (Map.Entry<Integer, Integer> entry : hashMap.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; ++i) {
			hashMap.put(in.readInt(), in.readInt());
		}
	}

}
