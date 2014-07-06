/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.typeutils.runtime;

import static org.junit.Assert.assertEquals;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.DoubleComparator;
import eu.stratosphere.api.common.typeutils.base.DoubleSerializer;
import eu.stratosphere.api.common.typeutils.base.IntComparator;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.common.typeutils.base.LongComparator;
import eu.stratosphere.api.common.typeutils.base.LongSerializer;
import eu.stratosphere.api.common.typeutils.base.StringComparator;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.runtime.tuple.base.TupleComparatorTestBase;

public class TupleComparatorTTT3Test extends TupleComparatorTestBase<Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>>{
	@SuppressWarnings("unchecked")
	Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>[] dataISD = new Tuple3[]{
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("hello", 1.0), new Tuple2<Long, Long>(1L, 1L), new Tuple2<Integer, Long>(4, -10L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("hello", 2.0), new Tuple2<Long, Long>(1L, 2L), new Tuple2<Integer, Long>(4, -5L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("hello", 3.0), new Tuple2<Long, Long>(1L, 3L), new Tuple2<Integer, Long>(4, 0L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("hello", 3.5), new Tuple2<Long, Long>(1L, 4L), new Tuple2<Integer, Long>(4, 5L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("hello", 4325.12), new Tuple2<Long, Long>(1L, 5L), new Tuple2<Integer, Long>(4, 15L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("world", 1.0), new Tuple2<Long, Long>(2L, 4L), new Tuple2<Integer, Long>(45, -5L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("world", 2.0), new Tuple2<Long, Long>(2L, 6L), new Tuple2<Integer, Long>(45, 5L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("world", 3.0), new Tuple2<Long, Long>(2L, 8L), new Tuple2<Integer, Long>(323, 2L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("world", 3.5), new Tuple2<Long, Long>(2L, 9L), new Tuple2<Integer, Long>(323, 5L)),
		new Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>(new Tuple2<String, Double>("world", 4325.12), new Tuple2<Long, Long>(2L, 123L), new Tuple2<Integer, Long>(555, 1L))
		
	};
	
	@SuppressWarnings("unchecked")
	@Override
	protected TupleComparator<Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>> createComparator(
			boolean ascending) {
		return new TupleComparator<Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>>(
				new int[] { 0, 2 },
				new TypeComparator[] {
						new TupleComparator<Tuple2<String, Double>>(new int[] {
								0, 1 }, new TypeComparator[] {
								new StringComparator(ascending),
								new DoubleComparator(ascending) },
								new TypeSerializer[] {
										StringSerializer.INSTANCE,
										DoubleSerializer.INSTANCE }),
						new TupleComparator<Tuple2<Long, Long>>(new int[] { 0,
								1 }, new TypeComparator[] {
								new LongComparator(ascending),
								new LongComparator(ascending) },
								new TypeSerializer[] { LongSerializer.INSTANCE,
										LongSerializer.INSTANCE }),
						new TupleComparator<Tuple2<Integer, Long>>(new int[] {
								0, 1 }, new TypeComparator[] {
								new IntComparator(ascending),
								new LongComparator(ascending) },
								new TypeSerializer[] { IntSerializer.INSTANCE,
										LongSerializer.INSTANCE }) },
				new TypeSerializer[] {
						new TupleSerializer<Tuple2<String, Double>>(
								(Class<Tuple2<String, Double>>) (Class<?>) Tuple2.class,
								new TypeSerializer[] {
										StringSerializer.INSTANCE,
										DoubleSerializer.INSTANCE }),
						new TupleSerializer<Tuple2<Long, Long>>(
								(Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class,
								new TypeSerializer[] {
										LongSerializer.INSTANCE,
										LongSerializer.INSTANCE }),
						new TupleSerializer<Tuple2<Integer, Long>>(
								(Class<Tuple2<Integer, Long>>) (Class<?>) Tuple2.class,
								new TypeSerializer[] {
										IntSerializer.INSTANCE,
										LongSerializer.INSTANCE }) });
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TupleSerializer<Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>> createSerializer() {
		return new  TupleSerializer<Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>>(
				(Class<Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>>) (Class<?>) Tuple3.class,
				new TypeSerializer[]{
					new TupleSerializer<Tuple2<String, Double>> ( (Class<Tuple2<String, Double>>) (Class<?>) Tuple2.class,
							new TypeSerializer[]{
						StringSerializer.INSTANCE,
						DoubleSerializer.INSTANCE
					}),
					new TupleSerializer<Tuple2<Long, Long>> ( (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class,
							new TypeSerializer[]{
						LongSerializer.INSTANCE,
						LongSerializer.INSTANCE
					}),
					new TupleSerializer<Tuple2<Integer, Long>> ( (Class<Tuple2<Integer, Long>>) (Class<?>) Tuple2.class,
							new TypeSerializer[]{
						IntSerializer.INSTANCE,
						LongSerializer.INSTANCE
					})
				});
	}

	@Override
	protected Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>>[] getSortedTestData() {
		return this.dataISD;
	}
	
	@Override
	protected void deepEquals(
			String message,
			Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>> should,
			Tuple3<Tuple2<String, Double>, Tuple2<Long, Long>, Tuple2<Integer, Long>> is) {
		
		for (int x = 0; x < should.getArity(); x++) {
			// Check whether field is of type Tuple because assertEquals with Tuples would compare the Objects.
			if(	should.getField(x) instanceof Tuple2) {
				this.deepEquals(message, (Tuple2<?,?>) should.getField(x), (Tuple2<?,?>)is.getField(x));
			}
			else {
				assertEquals(should.getField(x), is.getField(x));
			}
			
		}
	}
	
	protected void deepEquals(String message, Tuple2<?,?> should, Tuple2<?,?> is) {
		for (int x = 0; x < should.getArity(); x++) {
			assertEquals(should.getField(x), is.getField(x));
		}
	}
}
