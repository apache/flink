/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.Deserializer;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Serializer;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Serialization factory for {@link IOReadableWritable}s.
 * 
 * @author Erik Nijkamp
 * @author Stephan Ewen
 * @param <T>
 */
public class WritableSerializationFactory<T extends IOReadableWritable> implements SerializationFactory<T> {
	private final Class<T> clazz;

	public WritableSerializationFactory(Class<T> clazz) {
		this.clazz = clazz;
	}

	@Override
	public Deserializer<T> getDeserializer() {
		return new Deserializer<T>() {
			private DataInput input;

			@Override
			public void open(DataInput input) throws IOException {
				this.input = input;
			}

			@Override
			public void close() throws IOException {

			}

			@Override
			public T deserialize(T readable) throws IOException {
				// mutable deserialization
				if (readable == null) {
					readable = newInstance();
				}
				readable.read(input);
				return readable;
			}
		};
	}

	@Override
	public Serializer<T> getSerializer() {
		return new Serializer<T>() {
			private DataOutput output;

			@Override
			public void open(DataOutput output) throws IOException {
				this.output = output;
			}

			@Override
			public void close() throws IOException {

			}

			@Override
			public void serialize(T t) throws IOException {
				t.write(output);
			}
		};
	}

	@Override
	public T newInstance() {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.SerializationFactory#getRawComparator()
	 */
	@Override
	public RawComparator getRawComparator() {
		return rawComparators.get(clazz);
	}

	// ------------------------------------------------------------------------

	private static final Map<Class<? extends IOReadableWritable>, RawComparator> rawComparators = 
		new HashMap<Class<? extends IOReadableWritable>, RawComparator>(10);

	static {
		rawComparators.put(PactInteger.class, new PactIntergerBigEndianComparator());
		rawComparators.put(PactLong.class, new PactLongBigEndianComparator());
		rawComparators.put(PactDouble.class, new PactDoubleBigEndianComparator());
		rawComparators.put(PactString.class, new PactStringComparator());
	}

	private static final class PactIntergerBigEndianComparator implements RawComparator {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.iomanager.RawComparator#compare(byte[], byte[], int, int, int, int)
		 */
		@Override
		public int compare(byte[] key1, byte[] key2, int start1, int start2) {
			for (int i = 0; i < 4; i++) {
				byte b1 = key1[start1 + i];
				byte b2 = key2[start2 + i];

				if (b1 < b2) {
					return -1;
				} else if (b1 > b2) {
					return 1;
				}
			}

			return 0;
		}
	}

	private static final class PactLongBigEndianComparator implements RawComparator {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.iomanager.RawComparator#compare(byte[], byte[], int, int, int, int)
		 */
		@Override
		public int compare(byte[] key1, byte[] key2, int start1, int start2) {
			for (int i = 0; i < 8; i++) {
				byte b1 = key1[start1 + i];
				byte b2 = key2[start2 + i];

				if (b1 < b2) {
					return -1;
				} else if (b1 > b2) {
					return 1;
				}
			}

			return 0;
		}
	}

	private static final class PactDoubleBigEndianComparator implements RawComparator {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.iomanager.RawComparator#compare(byte[], byte[], int, int, int, int)
		 */
		@Override
		public int compare(byte[] key1, byte[] key2, int start1, int start2) {
			long bits1 = (((long) key1[start1] & 0xff) << 56) | (((long) key1[start1 + 1] & 0xff) << 48)
				| (((long) key1[start1 + 2] & 0xff) << 40) | (((long) key1[start1 + 3] & 0xff) << 32)
				| (((long) key1[start1 + 4] & 0xff) << 24) | (((long) key1[start1 + 5] & 0xff) << 16)
				| (((long) key1[start1 + 6] & 0xff) << 8) | (((long) key1[start1 + 7] & 0xff) << 0);

			long bits2 = (((long) key2[start2] & 0xff) << 56) | (((long) key2[start2 + 1] & 0xff) << 48)
				| (((long) key2[start2 + 2] & 0xff) << 40) | (((long) key2[start2 + 3] & 0xff) << 32)
				| (((long) key2[start2 + 4] & 0xff) << 24) | (((long) key2[start2 + 5] & 0xff) << 16)
				| (((long) key2[start2 + 6] & 0xff) << 8) | (((long) key2[start2 + 7] & 0xff) << 0);

			double d1 = Double.longBitsToDouble(bits1);
			double d2 = Double.longBitsToDouble(bits2);

			return d1 < d2 ? -1 : d1 > d2 ? 1 : 0;
		}
	}

	private static final class PactStringComparator implements RawComparator {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.iomanager.RawComparator#compare(byte[], byte[], int, int, int, int)
		 */
		@Override
		public int compare(byte[] key1, byte[] key2, int start1, int start2) {
			final int maxBit = 0x1 << 7;

			int l1 = key1[start1++];
			if (l1 >= maxBit) {
				int shift = 7;
				int curr;

				l1 = l1 & 0x7f;

				while ((curr = key1[start1++]) >= maxBit) {
					l1 |= (curr & 0x7f) << shift;
					shift += 7;
				}

				l1 |= curr << shift;
			}

			int l2 = key1[start2++];
			if (l2 >= maxBit) {
				int shift = 7;
				int curr;

				l2 = l2 & 0x7f;

				while ((curr = key2[start2++]) >= maxBit) {
					l2 |= (curr & 0x7f) << shift;
					shift += 7;
				}

				l2 |= curr << shift;
			}

			int ll = Math.min(l1, l2);
			int l = 0;
			while (l < ll) {
				int c1 = key1[start1++];
				if (c1 >= maxBit) {
					int shift = 7;
					int curr;
					c1 = c1 & 0x7f;
					while ((curr = key1[start1++]) >= maxBit) {
						c1 |= (curr & 0x7f) << shift;
						shift += 7;
					}

					c1 |= curr << shift;
				}

				int c2 = key2[start2++];
				if (c2 >= maxBit) {
					int shift = 7;
					int curr;
					c2 = c2 & 0x7f;
					while ((curr = key2[start2++]) >= maxBit) {
						c2 |= (curr & 0x7f) << shift;
						shift += 7;
					}

					c2 |= curr << shift;
				}

				l++;

				char cc1 = (char) c1;
				char cc2 = (char) c2;

				if (cc1 < cc2) {
					return -1;
				} else if (cc1 > cc2) {
					return 1;
				}
			}

			return l1 < l2 ? -1 : l1 > l2 ? 1 : 0;

		}
	}
}
