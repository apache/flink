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

package org.apache.flink.api.java.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.SampleInCoordinator;
import org.apache.flink.api.java.functions.SampleInPartition;
import org.apache.flink.api.java.functions.SampleWithFraction;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.summarize.aggregation.SummaryAggregatorFactory;
import org.apache.flink.api.java.summarize.aggregation.TupleSummaryAggregator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This class provides simple utility methods for zipping elements in a data set with an index
 * or with a unique identifier.
 */
@PublicEvolving
public final class DataSetUtils {

	/**
	 * Method that goes over all the elements in each partition in order to retrieve
	 * the total number of elements.
	 *
	 * @param input the DataSet received as input
	 * @return a data set containing tuples of subtask index, number of elements mappings.
	 */
	public static <T> DataSet<Tuple2<Integer, Long>> countElementsPerPartition(DataSet<T> input) {
		return input.mapPartition(new RichMapPartitionFunction<T, Tuple2<Integer, Long>>() {
			@Override
			public void mapPartition(Iterable<T> values, Collector<Tuple2<Integer, Long>> out) throws Exception {
				long counter = 0;
				for (T value : values) {
					counter++;
				}
				out.collect(new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), counter));
			}
		});
	}

	/**
	 * Method that assigns a unique {@link Long} value to all elements in the input data set. The generated values are
	 * consecutive.
	 *
	 * @param input the input data set
	 * @return a data set of tuple 2 consisting of consecutive ids and initial values.
	 */
	public static <T> DataSet<Tuple2<Long, T>> zipWithIndex(DataSet<T> input) {

		DataSet<Tuple2<Integer, Long>> elementCount = countElementsPerPartition(input);

		return input.mapPartition(new RichMapPartitionFunction<T, Tuple2<Long, T>>() {

			long start = 0;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);

				List<Tuple2<Integer, Long>> offsets = getRuntimeContext().getBroadcastVariableWithInitializer(
						"counts",
						new BroadcastVariableInitializer<Tuple2<Integer, Long>, List<Tuple2<Integer, Long>>>() {
							@Override
							public List<Tuple2<Integer, Long>> initializeBroadcastVariable(Iterable<Tuple2<Integer, Long>> data) {
								// sort the list by task id to calculate the correct offset
								List<Tuple2<Integer, Long>> sortedData = new ArrayList<>();
								for (Tuple2<Integer, Long> datum : data) {
									sortedData.add(datum);
								}
								Collections.sort(sortedData, new Comparator<Tuple2<Integer, Long>>() {
									@Override
									public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
										return o1.f0.compareTo(o2.f0);
									}
								});
								return sortedData;
							}
						});

				// compute the offset for each partition
				for (int i = 0; i < getRuntimeContext().getIndexOfThisSubtask(); i++) {
					start += offsets.get(i).f1;
				}
			}

			@Override
			public void mapPartition(Iterable<T> values, Collector<Tuple2<Long, T>> out) throws Exception {
				for (T value: values) {
					out.collect(new Tuple2<>(start++, value));
				}
			}
		}).withBroadcastSet(elementCount, "counts");
	}

	/**
	 * Method that assigns a unique {@link Long} value to all elements in the input data set as described below.
	 * <ul>
	 *  <li> a map function is applied to the input data set
	 *  <li> each map task holds a counter c which is increased for each record
	 *  <li> c is shifted by n bits where n = log2(number of parallel tasks)
	 * 	<li> to create a unique ID among all tasks, the task id is added to the counter
	 * 	<li> for each record, the resulting counter is collected
	 * </ul>
	 *
	 * @param input the input data set
	 * @return a data set of tuple 2 consisting of ids and initial values.
	 */
	public static <T> DataSet<Tuple2<Long, T>> zipWithUniqueId (DataSet <T> input) {

		return input.mapPartition(new RichMapPartitionFunction<T, Tuple2<Long, T>>() {

			long maxBitSize = getBitSize(Long.MAX_VALUE);
			long shifter = 0;
			long start = 0;
			long taskId = 0;
			long label = 0;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				shifter = getBitSize(getRuntimeContext().getNumberOfParallelSubtasks() - 1);
				taskId = getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public void mapPartition(Iterable<T> values, Collector<Tuple2<Long, T>> out) throws Exception {
				for (T value : values) {
					label = (start << shifter) + taskId;

					if (getBitSize(start) + shifter < maxBitSize) {
						out.collect(new Tuple2<>(label, value));
						start++;
					} else {
						throw new Exception("Exceeded Long value range while generating labels");
					}
				}
			}
		});
	}

	// --------------------------------------------------------------------------------------------
	//  Sample
	// --------------------------------------------------------------------------------------------

	/**
	 * Generate a sample of DataSet by the probability fraction of each element.
	 *
	 * @param withReplacement Whether element can be selected more than once.
	 * @param fraction        Probability that each element is chosen, should be [0,1] without replacement,
	 *                        and [0, ∞) with replacement. While fraction is larger than 1, the elements are
	 *                        expected to be selected multi times into sample on average.
	 * @return The sampled DataSet
	 */
	public static <T> MapPartitionOperator<T, T> sample(
		DataSet <T> input,
		final boolean withReplacement,
		final double fraction) {

		return sample(input, withReplacement, fraction, Utils.RNG.nextLong());
	}

	/**
	 * Generate a sample of DataSet by the probability fraction of each element.
	 *
	 * @param withReplacement Whether element can be selected more than once.
	 * @param fraction        Probability that each element is chosen, should be [0,1] without replacement,
	 *                        and [0, ∞) with replacement. While fraction is larger than 1, the elements are
	 *                        expected to be selected multi times into sample on average.
	 * @param seed            random number generator seed.
	 * @return The sampled DataSet
	 */
	public static <T> MapPartitionOperator<T, T> sample(
		DataSet <T> input,
		final boolean withReplacement,
		final double fraction,
		final long seed) {

		return input.mapPartition(new SampleWithFraction<T>(withReplacement, fraction, seed));
	}

	/**
	 * Generate a sample of DataSet which contains fixed size elements.
	 *
	 * <p><strong>NOTE:</strong> Sample with fixed size is not as efficient as sample with fraction, use sample with
	 * fraction unless you need exact precision.
	 *
	 * @param withReplacement Whether element can be selected more than once.
	 * @param numSamples       The expected sample size.
	 * @return The sampled DataSet
	 */
	public static <T> DataSet<T> sampleWithSize(
		DataSet <T> input,
		final boolean withReplacement,
		final int numSamples) {

		return sampleWithSize(input, withReplacement, numSamples, Utils.RNG.nextLong());
	}

	/**
	 * Generate a sample of DataSet which contains fixed size elements.
	 *
	 * <p><strong>NOTE:</strong> Sample with fixed size is not as efficient as sample with fraction, use sample with
	 * fraction unless you need exact precision.
	 *
	 * @param withReplacement Whether element can be selected more than once.
	 * @param numSamples       The expected sample size.
	 * @param seed            Random number generator seed.
	 * @return The sampled DataSet
	 */
	public static <T> DataSet<T> sampleWithSize(
		DataSet <T> input,
		final boolean withReplacement,
		final int numSamples,
		final long seed) {

		SampleInPartition<T> sampleInPartition = new SampleInPartition<>(withReplacement, numSamples, seed);
		MapPartitionOperator mapPartitionOperator = input.mapPartition(sampleInPartition);

		// There is no previous group, so the parallelism of GroupReduceOperator is always 1.
		String callLocation = Utils.getCallLocationName();
		SampleInCoordinator<T> sampleInCoordinator = new SampleInCoordinator<>(withReplacement, numSamples, seed);
		return new GroupReduceOperator<>(mapPartitionOperator, input.getType(), sampleInCoordinator, callLocation);
	}

	// --------------------------------------------------------------------------------------------
	//  Partition
	// --------------------------------------------------------------------------------------------

	/**
	 * Range-partitions a DataSet on the specified tuple field positions.
	 */
	public static <T> PartitionOperator<T> partitionByRange(DataSet<T> input, DataDistribution distribution, int... fields) {
		return new PartitionOperator<>(input, PartitionOperatorBase.PartitionMethod.RANGE, new Keys.ExpressionKeys<>(fields, input.getType(), false), distribution, Utils.getCallLocationName());
	}

	/**
	 * Range-partitions a DataSet on the specified fields.
	 */
	public static <T> PartitionOperator<T> partitionByRange(DataSet<T> input, DataDistribution distribution, String... fields) {
		return new PartitionOperator<>(input, PartitionOperatorBase.PartitionMethod.RANGE, new Keys.ExpressionKeys<>(fields, input.getType()), distribution, Utils.getCallLocationName());
	}

	/**
	 * Range-partitions a DataSet using the specified key selector function.
	 */
	public static <T, K extends Comparable<K>> PartitionOperator<T> partitionByRange(DataSet<T> input, DataDistribution distribution, KeySelector<T, K> keyExtractor) {
		final TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keyExtractor, input.getType());
		return new PartitionOperator<>(input, PartitionOperatorBase.PartitionMethod.RANGE, new Keys.SelectorFunctionKeys<>(input.clean(keyExtractor), input.getType(), keyType), distribution, Utils.getCallLocationName());
	}

	// --------------------------------------------------------------------------------------------
	//  Summarize
	// --------------------------------------------------------------------------------------------

	/**
	 * Summarize a DataSet of Tuples by collecting single pass statistics for all columns.
	 *
	 * <p>Example usage:
	 * <pre>
	 * {@code
	 * Dataset<Tuple3<Double, String, Boolean>> input = // [...]
	 * Tuple3<NumericColumnSummary,StringColumnSummary, BooleanColumnSummary> summary = DataSetUtils.summarize(input)
	 *
	 * summary.f0.getStandardDeviation()
	 * summary.f1.getMaxLength()
	 * }
	 * </pre>
	 * @return the summary as a Tuple the same width as input rows
	 */
	public static <R extends Tuple, T extends Tuple> R summarize(DataSet<T> input) throws Exception {
		if (!input.getType().isTupleType()) {
			throw new IllegalArgumentException("summarize() is only implemented for DataSet's of Tuples");
		}
		final TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) input.getType();
		DataSet<TupleSummaryAggregator<R>> result = input.mapPartition(new MapPartitionFunction<T, TupleSummaryAggregator<R>>() {
			@Override
			public void mapPartition(Iterable<T> values, Collector<TupleSummaryAggregator<R>> out) throws Exception {
				TupleSummaryAggregator<R> aggregator = SummaryAggregatorFactory.create(inType);
				for (Tuple value : values) {
					aggregator.aggregate(value);
				}
				out.collect(aggregator);
			}
		}).reduce(new ReduceFunction<TupleSummaryAggregator<R>>() {
			@Override
			public TupleSummaryAggregator<R> reduce(TupleSummaryAggregator<R> agg1, TupleSummaryAggregator<R> agg2) throws Exception {
				agg1.combine(agg2);
				return agg1;
			}
		});
		return result.collect().get(0).result();
	}

	// --------------------------------------------------------------------------------------------
	//  Checksum
	// --------------------------------------------------------------------------------------------

	/**
	 * Convenience method to get the count (number of elements) of a DataSet
	 * as well as the checksum (sum over element hashes).
	 *
	 * @return A ChecksumHashCode that represents the count and checksum of elements in the data set.
	 * @deprecated replaced with {@code org.apache.flink.graph.asm.dataset.ChecksumHashCode} in Gelly
	 */
	@Deprecated
	public static <T> Utils.ChecksumHashCode checksumHashCode(DataSet<T> input) throws Exception {
		final String id = new AbstractID().toString();

		input.output(new Utils.ChecksumHashCodeHelper<T>(id)).name("ChecksumHashCode");

		JobExecutionResult res = input.getExecutionEnvironment().execute();
		return res.<Utils.ChecksumHashCode> getAccumulatorResult(id);
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	public static int getBitSize(long value) {
		if (value > Integer.MAX_VALUE) {
			return 64 - Integer.numberOfLeadingZeros((int) (value >> 32));
		} else {
			return 32 - Integer.numberOfLeadingZeros((int) value);
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private DataSetUtils() {
		throw new RuntimeException();
	}
}
