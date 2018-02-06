package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;

/**
 * Created by zsh on 06/02/2018.
 */
public class PartitionedBloomFilterDescriptor<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final long DEFAULT_MAX_EXPECT_NUM = 128_000_000;
	private static final long DEFAULT_MIN_EXPECT_NUMBER = 1000_000;
	private static final double DEFAULT_GROW_RATE = 2.0;

	private String stateName;
	private long capacity;
	private long ttl;

	// false positive probability
	private double fpp;

	private long miniExpectNum;
	private long maxExpectNum;
	private double growRate;

	/** The type information describing the value type. Only used to lazily create the serializer
	 * and dropped during serialization */
	private transient TypeInformation<T> typeInfo;

	/** The serializer for the type. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	protected TypeSerializer<T> serializer;

	public PartitionedBloomFilterDescriptor(String stateName, TypeInformation<T> typeInfo, long capacity, double fpp) {
		this(
			stateName,
			typeInfo,
			capacity,
			fpp,
			Integer.MAX_VALUE);
	}

	public PartitionedBloomFilterDescriptor(String stateName, TypeInformation<T> typeInfo, long capacity, double fpp, long ttl) {
		this(
			stateName,
			typeInfo,
			capacity,
			fpp,
			ttl,
			DEFAULT_MIN_EXPECT_NUMBER,
			DEFAULT_MAX_EXPECT_NUM,
			DEFAULT_GROW_RATE);
	}

	public PartitionedBloomFilterDescriptor(String stateName, TypeInformation<T> typeInfo, long capacity, double fpp, long ttl, long minExpectNum, long maxExpectNum, double growRate) {
		this.stateName = stateName;
		this.typeInfo = typeInfo;
		this.capacity = capacity;
		this.fpp = fpp;
		this.ttl = ttl;
		this.miniExpectNum = minExpectNum;
		this.maxExpectNum = maxExpectNum;
		this.growRate = growRate;
	}

	public PartitionedBloomFilterDescriptor(String stateName, TypeSerializer<T> serializer, long capacity, double fpp, long ttl) {
		this(
			stateName,
			serializer,
			capacity,
			fpp,
			ttl,
			DEFAULT_MIN_EXPECT_NUMBER,
			DEFAULT_MAX_EXPECT_NUM,
			DEFAULT_GROW_RATE);
	}

	public PartitionedBloomFilterDescriptor(String stateName, TypeSerializer<T> serializer, long capacity, double fpp, long ttl, long minExpectNum, long maxExpectNum, double growRate) {
		this.stateName = stateName;
		this.serializer = serializer;
		this.capacity = capacity;
		this.fpp = fpp;
		this.ttl = ttl;
		this.miniExpectNum = minExpectNum;
		this.maxExpectNum = maxExpectNum;
		this.growRate = growRate;
	}

	public long getTtl() {
		return ttl;
	}

	public double getFpp() {
		return fpp;
	}

	public String getStateName() {
		return stateName;
	}

	public long getCapacity() {
		return capacity;
	}

	public long getMiniExpectNum() {
		return miniExpectNum;
	}

	public long getMaxExpectNum() {
		return maxExpectNum;
	}

	public double getGrowRate() {
		return growRate;
	}

//	public void snapshot(DataOutputViewStreamWrapper out) throws IOException {
//		out.writeUTF(stateName);
//		out.writeLong(capacity);
//		out.writeLong(ttl);
//		out.writeDouble(fpp);
//
//		out.writeLong(miniExpectNum);
//		out.writeLong(maxExpectNum);
//		out.writeDouble(growRate);
//	}
//
//	public static BloomFilterStateDescriptor restore(DataInputViewStreamWrapper in) throws IOException {
//		BloomFilterStateDescriptor desc = new BloomFilterStateDescriptor();
//		desc.stateName = in.readUTF();
//		desc.capacity = in.readLong();
//		desc.ttl = in.readLong();
//		desc.fpp = in.readDouble();
//
//		desc.miniExpectNum = in.readLong();
//		desc.maxExpectNum = in.readLong();
//		desc.growRate = in.readDouble();
//		return desc;
//	}

	public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
		if (serializer == null) {
			if (typeInfo != null) {
				serializer = typeInfo.createSerializer(executionConfig);
			} else {
				throw new IllegalStateException(
					"Cannot initialize serializer after TypeInformation was dropped during serialization");
			}
		}
	}

	public TypeSerializer<T> getSerializer() {
		if (serializer != null) {
			return serializer.duplicate();
		} else {
			throw new IllegalStateException("Serializer not yet initialized.");
		}
	}
}
