/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.aggfunctions.cardinality;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Java implementation of HyperLogLog (HLL) algorithm from this paper:
 * <p/>
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * <p/>
 * HLL is an improved version of LogLog that is capable of estimating
 * the cardinality of a set with accuracy = 1.04/sqrt(m) where
 * m = 2^b.  So we can control accuracy vs space usage by increasing
 * or decreasing b.
 * <p/>
 * The main benefit of using HLL over LL is that it only requires 64%
 * of the space that LL does to get the same accuracy.
 * <p/>
 * <p>
 * Note that this implementation does not include the long range correction function
 * defined in the original paper.  Empirical evidence shows that the correction
 * function causes more harm than good.
 * </p>
 */
public class HyperLogLog implements ICardinality, Serializable {

	private final RegisterSet registerSet;
	private final int log2m;
	private final double alphaMM;


	/**
	 * Create a new HyperLogLog instance using the specified standard deviation.
	 *
	 * @param rsd - the relative standard deviation for the counter.
	 *            smaller values create counters that require more space.
	 */
	public HyperLogLog(double rsd) {
		this(log2m(rsd));
	}

	private static int log2m(double rsd) {
		return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
	}

	private static double rsd(int log2m) {
		return 1.106 / Math.sqrt(Math.exp(log2m * Math.log(2)));
	}

	private static void validateLog2m(int log2m) {
		if (log2m < 0 || log2m > 30) {
			throw new IllegalArgumentException("log2m argument is "
					+ log2m + " and is outside the range [0, 30]");
		}
	}

	private static double linearCounting(int m, double v) {
		return m * Math.log(m / v);
	}

	/**
	 * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
	 * the counter.  The larger the log2m the better the accuracy.
	 * <p/>
	 * accuracy = 1.04/sqrt(2^log2m)
	 *
	 * @param log2m - the number of bits to use as the basis for the HLL instance
	 */
	public HyperLogLog(int log2m) {
		this(log2m, new RegisterSet(1 << log2m));
	}

	/**
	 * Creates a new HyperLogLog instance using the given registers.  Used for unmarshalling a serialized
	 * instance and for merging multiple counters together.
	 *
	 * @param registerSet - the initial values for the register set
	 */
	public HyperLogLog(int log2m, RegisterSet registerSet) {
		validateLog2m(log2m);
		this.registerSet = registerSet;
		this.log2m = log2m;
		int m = 1 << this.log2m;

		alphaMM = getAlphaMM(log2m, m);
	}

	@Override
	public boolean offerHashed(long hashedValue) {
		// j becomes the binary address determined by the first b log2m of x
		// j will be between 0 and 2^log2m
		final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
		final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
		return registerSet.updateIfGreater(j, r);
	}

	@Override
	public boolean offerHashed(int hashedValue) {
		// j becomes the binary address determined by the first b log2m of x
		// j will be between 0 and 2^log2m
		final int j = hashedValue >>> (Integer.SIZE - log2m);
		final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
		return registerSet.updateIfGreater(j, r);
	}

	@Override
	public boolean offer(Object o) {
		final int x = MurmurHash.hash(o);
		return offerHashed(x);
	}

	@Override
	public long cardinality() {
		double registerSum = 0;
		int count = registerSet.count;
		double zeros = 0.0;
		for (int j = 0; j < registerSet.count; j++) {
			int val = registerSet.get(j);
			registerSum += 1.0 / (1 << val);
			if (val == 0) {
				zeros++;
			}
		}

		double estimate = alphaMM * (1 / registerSum);

		if (estimate <= (5.0 / 2.0) * count) {
			// Small Range Estimate
			return Math.round(linearCounting(count, zeros));
		} else {
			return Math.round(estimate);
		}
	}

	@Override
	public int sizeof() {
		return registerSet.size * 4;
	}

	@Override
	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dos = new DataOutputStream(baos);
		writeBytes(dos);

		return baos.toByteArray();
	}

	private void writeBytes(DataOutput serializedByteStream) throws IOException {
		serializedByteStream.writeInt(log2m);
		serializedByteStream.writeInt(registerSet.size * 4);
		for (int x : registerSet.readOnlyBits()) {
			serializedByteStream.writeInt(x);
		}
	}

	/**
	 * Add all the elements of the other set to this set.
	 * <p/>
	 * This operation does not imply a loss of precision.
	 *
	 * @param other A compatible Hyperloglog instance (same log2m)
	 * @throws Exception if other is not compatible
	 */
	public void addAll(HyperLogLog other) throws Exception {
		if (this.sizeof() != other.sizeof()) {
			throw new Exception("Cannot merge estimators of different sizes");
		}

		registerSet.merge(other.registerSet);
	}

	@Override
	public ICardinality merge(ICardinality... estimators) throws Exception {
		HyperLogLog merged = new HyperLogLog(log2m, new RegisterSet(this.registerSet.count));
		merged.addAll(this);

		if (estimators == null) {
			return merged;
		}

		for (ICardinality estimator : estimators) {
			if (!(estimator instanceof HyperLogLog)) {
				throw new Exception("Cannot merge estimators of different class");
			}
			HyperLogLog hll = (HyperLogLog) estimator;
			merged.addAll(hll);
		}

		return merged;
	}

	private Object writeReplace() {
		return new SerializationHolder(this);
	}

	/**
	 * This class exists to support Externalizable semantics for
	 * HyperLogLog objects without having to expose a public
	 * constructor, public write/read methods, or pretend final
	 * fields aren't final.
	 *
	 * <p>
	 * In short, Externalizable allows you to skip some of the more
	 * verbose meta-data default Serializable gets you, but still
	 * includes the class name. In that sense, there is some cost
	 * to this holder object because it has a longer class name. I
	 * imagine people who care about optimizing for that have their
	 * own work-around for long class names in general, or just use
	 * a custom serialization framework. Therefore we make no attempt
	 * to optimize that here (eg. by raising this from an inner class
	 * and giving it an unhelpful name).
	 * </p>
	 */
	private static class SerializationHolder implements Externalizable {

		HyperLogLog hyperLogLogHolder;

		public SerializationHolder(HyperLogLog hyperLogLogHolder) {
			this.hyperLogLogHolder = hyperLogLogHolder;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			hyperLogLogHolder.writeBytes(out);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			hyperLogLogHolder = Builder.build(in);
		}

		private Object readResolve() {
			return hyperLogLogHolder;
		}
	}

	/**
	 * Build a HyperLogLog instance.
	 */
	public static class Builder implements Serializable {
		private static final long serialVersionUID = -2567898469253021883L;

		private final double rsd;
		private transient int log2m;

		/**
		 * Uses the given RSD percentage to determine how many bytes the constructed HyperLogLog will use.
		 */
		@Deprecated
		public Builder(double rsd) {
			this.log2m = log2m(rsd);
			validateLog2m(log2m);
			this.rsd = rsd;
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			this.log2m = log2m(rsd);
		}

		public HyperLogLog build() {
			return new HyperLogLog(log2m);
		}

		public static HyperLogLog build(byte[] bytes) throws IOException {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			return build(new DataInputStream(bais));
		}

		public static HyperLogLog build(DataInput serializedByteStream) throws IOException {
			int log2m = serializedByteStream.readInt();
			int byteArraySize = serializedByteStream.readInt();
			return new HyperLogLog(log2m,
					new RegisterSet(1 << log2m, getBits(serializedByteStream, byteArraySize)));
		}
	}

	/**
	 * Byte array to int array.
	 */
	public static int[] getBits(DataInput dataIn, int byteLength) throws IOException {
		int bitSize = byteLength / 4;
		int[] bits = new int[bitSize];
		for (int i = 0; i < bitSize; i++) {
			bits[i] = dataIn.readInt();
		}
		return bits;
	}

	protected static double getAlphaMM(final int p, final int m) {
		// See the paper.
		switch (p) {
			case 4:
				return 0.673 * m * m;
			case 5:
				return 0.697 * m * m;
			case 6:
				return 0.709 * m * m;
			default:
				return (0.7213 / (1 + 1.079 / m)) * m * m;
		}
	}

}
