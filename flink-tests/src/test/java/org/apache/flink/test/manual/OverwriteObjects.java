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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.types.IntValue;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/*
 * These programs demonstrate the effects of user defined functions which modify input objects or return locally created
 * objects that are retained and reused on future calls. The programs do not retain and later modify input objects.
 */
public class OverwriteObjects {

	public final static Logger LOG = LoggerFactory.getLogger(OverwriteObjects.class);

	// DataSets are created with this number of elements
	private static final int NUMBER_OF_ELEMENTS = 3 * 1000 * 1000;

	// DataSet values are randomly generated over this range
	private static final int KEY_RANGE = 1 * 1000 * 1000;

	private static final int MAX_PARALLELISM = 4;

	private static final long RANDOM_SEED = new Random().nextLong();

	public static void main(String[] args) throws Exception {
		new OverwriteObjects().run();
	}

	public void run() throws Exception {
		LOG.info("Random seed = {}", RANDOM_SEED);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		for (int parallelism = MAX_PARALLELISM ; parallelism > 0 ; parallelism--) {
			LOG.info("Parallelism = {}", parallelism);

			env.setParallelism(parallelism);

			testReduce(env);
			testGroupedReduce(env);
			testJoin(env);
		}
	}

	// --------------------------------------------------------------------------------------------

	public void testReduce(ExecutionEnvironment env) throws Exception {
		/*
		 * Test ChainedAllReduceDriver
		 */

		LOG.info("Testing reduce");

		env.getConfig().enableObjectReuse();

		Tuple2<IntValue, IntValue> enabledResult = getDataSet(env)
			.reduce(new OverwriteObjectsReduce(false))
			.collect()
			.get(0);

		env.getConfig().disableObjectReuse();

		Tuple2<IntValue, IntValue> disabledResult = getDataSet(env)
			.reduce(new OverwriteObjectsReduce(false))
			.collect()
			.get(0);

		Assert.assertEquals(NUMBER_OF_ELEMENTS, enabledResult.f1.getValue());
		Assert.assertEquals(NUMBER_OF_ELEMENTS, disabledResult.f1.getValue());

		Assert.assertEquals(disabledResult, enabledResult);
	}

	public void testGroupedReduce(ExecutionEnvironment env) throws Exception {
		/*
		 * Test ReduceCombineDriver and ReduceDriver
		 */

		LOG.info("Testing grouped reduce");

		env.getConfig().enableObjectReuse();

		ChecksumHashCode enabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
			.groupBy(0)
			.reduce(new OverwriteObjectsReduce(true)));

		env.getConfig().disableObjectReuse();

		ChecksumHashCode disabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
			.groupBy(0)
			.reduce(new OverwriteObjectsReduce(true)));

		Assert.assertEquals(disabledChecksum, enabledChecksum);
	}

	public class OverwriteObjectsReduce implements ReduceFunction<Tuple2<IntValue, IntValue>> {
		private Scrambler scrambler;

		public OverwriteObjectsReduce(boolean keyed) {
			scrambler = new Scrambler(keyed);
		}

		@Override
		public Tuple2<IntValue, IntValue> reduce(Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) throws Exception {
			return scrambler.scramble(a, b);
		}
	}

	// --------------------------------------------------------------------------------------------

	public void testJoin(ExecutionEnvironment env) throws Exception {
		/*
		 * Test JoinDriver, LeftOuterJoinDriver, RightOuterJoinDriver, and FullOuterJoinDriver
		 */

		for (JoinHint joinHint : JoinHint.values()) {
			if (joinHint == JoinHint.OPTIMIZER_CHOOSES) {
				continue;
			}

			ChecksumHashCode enabledChecksum;

			ChecksumHashCode disabledChecksum;

			// Inner join

			LOG.info("Testing inner join with JoinHint = {}", joinHint);

			env.getConfig().enableObjectReuse();

			enabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
				.join(getDataSet(env), joinHint)
				.where(0)
				.equalTo(0)
				.with(new OverwriteObjectsJoin()));

			env.getConfig().disableObjectReuse();

			disabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
				.join(getDataSet(env), joinHint)
				.where(0)
				.equalTo(0)
				.with(new OverwriteObjectsJoin()));

			Assert.assertEquals("JoinHint=" + joinHint, disabledChecksum, enabledChecksum);

			// Left outer join

			if (joinHint != JoinHint.BROADCAST_HASH_FIRST) {
				LOG.info("Testing left outer join with JoinHint = {}", joinHint);

				env.getConfig().enableObjectReuse();

				enabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
					.leftOuterJoin(getFilteredDataSet(env), joinHint)
					.where(0)
					.equalTo(0)
					.with(new OverwriteObjectsJoin()));

				env.getConfig().disableObjectReuse();

				disabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
					.leftOuterJoin(getFilteredDataSet(env), joinHint)
					.where(0)
					.equalTo(0)
					.with(new OverwriteObjectsJoin()));

				Assert.assertEquals("JoinHint=" + joinHint, disabledChecksum, enabledChecksum);
			}

			// Right outer join

			if (joinHint != JoinHint.BROADCAST_HASH_SECOND) {
				LOG.info("Testing right outer join with JoinHint = {}", joinHint);

				env.getConfig().enableObjectReuse();

				enabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
					.rightOuterJoin(getFilteredDataSet(env), joinHint)
					.where(0)
					.equalTo(0)
					.with(new OverwriteObjectsJoin()));

				env.getConfig().disableObjectReuse();

				disabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
					.rightOuterJoin(getFilteredDataSet(env), joinHint)
					.where(0)
					.equalTo(0)
					.with(new OverwriteObjectsJoin()));

				Assert.assertEquals("JoinHint=" + joinHint, disabledChecksum, enabledChecksum);
			}

			// Full outer join

			if (joinHint != JoinHint.BROADCAST_HASH_FIRST && joinHint != JoinHint.BROADCAST_HASH_SECOND) {
				LOG.info("Testing full outer join with JoinHint = {}", joinHint);

				env.getConfig().enableObjectReuse();

				enabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
					.fullOuterJoin(getFilteredDataSet(env), joinHint)
					.where(0)
					.equalTo(0)
					.with(new OverwriteObjectsJoin()));

				env.getConfig().disableObjectReuse();

				disabledChecksum = DataSetUtils.checksumHashCode(getDataSet(env)
					.fullOuterJoin(getFilteredDataSet(env), joinHint)
					.where(0)
					.equalTo(0)
					.with(new OverwriteObjectsJoin()));

				Assert.assertEquals("JoinHint=" + joinHint, disabledChecksum, enabledChecksum);
			}
		}
	}

	public class OverwriteObjectsJoin implements JoinFunction<Tuple2<IntValue, IntValue>, Tuple2<IntValue, IntValue>, Tuple2<IntValue, IntValue>> {
		private Scrambler scrambler = new Scrambler(true);

		@Override
		public Tuple2<IntValue, IntValue> join(Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) throws Exception {
			return scrambler.scramble(a, b);
		}
	}

	// --------------------------------------------------------------------------------------------

	private DataSet<Tuple2<IntValue, IntValue>> getDataSet(ExecutionEnvironment env) {
		return env
			.fromCollection(new TupleIntValueIntValueIterator(NUMBER_OF_ELEMENTS, KEY_RANGE),
				TupleTypeInfo.<Tuple2<IntValue, IntValue>>getBasicAndBasicValueTupleTypeInfo(IntValue.class, IntValue.class));
	}

	private DataSet<Tuple2<IntValue, IntValue>> getFilteredDataSet(ExecutionEnvironment env) {
		return getDataSet(env)
			.filter(new FilterFunction<Tuple2<IntValue, IntValue>>() {
				@Override
				public boolean filter(Tuple2<IntValue, IntValue> value) throws Exception {
					return (value.f0.getValue() % 2) == 0;
				}
			});
	}

	private static final class TupleIntValueIntValueIterator implements Iterator<Tuple2<IntValue, IntValue>>, Serializable {

		private int numElements;
		private final int keyRange;
		private Tuple2<IntValue, IntValue> ret = new Tuple2<>(new IntValue(), new IntValue());

		public TupleIntValueIntValueIterator(int numElements, int keyRange) {
			this.numElements = numElements;
			this.keyRange = keyRange;
		}

		private final Random rnd = new Random(123);

		@Override
		public boolean hasNext() {
			return numElements > 0;
		}

		@Override
		public Tuple2<IntValue, IntValue> next() {
			numElements--;
			ret.f0.setValue(rnd.nextInt(keyRange));
			ret.f1.setValue(1);
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class Scrambler implements Serializable {
		private Tuple2<IntValue, IntValue> d = new Tuple2<>(new IntValue(), new IntValue());

		private final boolean keyed;

		public Scrambler(boolean keyed) {
			this.keyed = keyed;
		}

		public Tuple2<IntValue, IntValue> scramble(Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) {
			/*
			 * Scramble all fields except returned object's key
			 *
			 * Randomly select among four return values:
			 *
			 *   0) return first object (a)
			 *   1) return second object (b)
			 *   2) return new object
			 *   3) return reused local object (d)
			 */

			Random random = new Random(RANDOM_SEED);

			if (a != null && b != null) {
				random.setSeed((((long) a.f0.getValue()) << 32) + b.f0.getValue());
			} else if (a != null) {
				random.setSeed(a.f0.getValue());
			} else if (b != null) {
				random.setSeed(b.f0.getValue());
			} else {
				throw new RuntimeException("One of a or b should be not null");
			}

			Tuple2<IntValue, IntValue> result;

			switch (random.nextInt(4)) {
				case 0:
					result = a;
					break;
				case 1:
					result = b;
					break;
				case 2:
					result = d;
					break;
				case 3:
					result = new Tuple2<>(new IntValue(), new IntValue());
					break;
				default:
					throw new RuntimeException("Unexpected value in switch statement");
			}

			if (a == null || b == null) {
				// null values are seen when processing outer joins
				if (result == null) {
					result = d;
				}

				if (a == null) {
					b.f0.copyTo(result.f0);
					b.f1.copyTo(result.f1);
				} else {
					a.f0.copyTo(result.f0);
					a.f1.copyTo(result.f1);
				}
			} else {
				if (keyed) {
					result.f0.setValue(a.f0.getValue());
				} else {
					result.f0.setValue(a.f0.getValue() + b.f0.getValue());
				}

				result.f1.setValue(a.f1.getValue() + b.f1.getValue());
			}

			scrambleIfNot(a, result);
			scrambleIfNot(b, result);
			scrambleIfNot(d, result);

			return result;
		}

		private Random random = new Random(~RANDOM_SEED);

		private void scrambleIfNot(Tuple2<IntValue, IntValue> t, Object o) {
			// verify that the tuple is not null and the same as the
			// comparison object, then scramble the fields
			if (t != null && t != o) {
				t.f0.setValue(random.nextInt());
				t.f1.setValue(random.nextInt());
			}
		}
	}
}
