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


package org.apache.flink.examples.java.ap;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.examples.java.ap.util.AffinityPropagationData;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

/**
 * Created by joseprubio on 9/22/16.
 */

public class AffinityPropagationBulk {

	private static final double DAMPING_FACTOR = 0.9;
	private static final double CONVERGENCE_THRESHOLD = 0.12;
	private static final String CONVERGENCE_MESSAGES = "message convergence";

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		// Get input similarities Tuple3<src, target, similarity>
		DataSet<Tuple3<LongValue, LongValue, DoubleValue>> similarities =
			AffinityPropagationData.getTuplesFromFile(env);

		// Init input to iteration
		DataSet<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> initMessages
			= similarities.map(new InitMessage());

		// Iterate
		IterativeDataSet<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> messages
			= initMessages.iterate(20);

		// Create aggregator
		messages.registerAggregationConvergenceCriterion(CONVERGENCE_MESSAGES, new LongSumAggregator(),
			new MessageConvergence(similarities.count() * 2));

		// Start responsibility message calculation
		// r(i,k) <- s(i,k) - max {a(i,K) + s(i,K)} st K != k
		// Iterate over Tuple6 <Source, Target, Responsibility , Availability, IsExemplar, ConvergenceCounter>

		DataSet<Tuple3<LongValue, LongValue, DoubleValue>> responsibilities = similarities

			// Get a list of a(i,K) + s(i,K) values joining similarities with messages
			.join(messages).where("f0","f1").equalTo("f0","f1").with(new joinAvailabilitySimilarity())

			// Get a dataset with 2 higher values
			.groupBy("f1").sortGroup("f2", Order.DESCENDING).first(2)

			// Create a Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> reducing the 2 tuples with higher values
			.groupBy("f1").reduceGroup(new responsibilityReduceGroup())

			// Calculate the R messages "r(i,k) <- s(i,k) - value" getting "value" joining
			// similarities with previous tuple
			.leftOuterJoin(similarities).where("f0").equalTo("f1").with(new responsibilityValue())

			// Responsibility damping
			.join(messages).where("f0","f1").equalTo("f1","f0").with(new dampedRValue(DAMPING_FACTOR, CONVERGENCE_THRESHOLD));

		// Start availability message calculation
		// a(i,k) <- min {0, r(k,k) + sum{max{0,r(I,k)}} I st I not in {i,k}
		// a(k,k) <- sum{max{0,r(I,k)} I st I not in {i,k}

		DataSet<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> availabilities = responsibilities

			// Get the sum of the positive responsibilities and the self responsibility per target
			.groupBy("f1").reduceGroup(new availabilityReduceGroup())

			// Calculate the availability
			.leftOuterJoin(responsibilities).where("f0").equalTo("f1").with(new availabilityValue())

			// Availability damping
			.join(messages).where("f0","f1").equalTo("f0","f1").with(new dampedAValue(DAMPING_FACTOR, CONVERGENCE_THRESHOLD));

		// End iteration
		DataSet<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> finalMessages =
			messages.closeWith(availabilities);

		// Get exemplars
		DataSet<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>>
			exemplars = finalMessages.filter(new FilterExemplars());

		// Get clusters
		DataSet<Tuple3<LongValue, LongValue, DoubleValue>> clusters = exemplars
				.join(similarities).where("f0").equalTo("f1").projectSecond(0,1,2);

		// Refine clusters assigning exemplars to themselves
		DataSet<Tuple3<LongValue, LongValue, DoubleValue>> refinedClusters = clusters
			.groupBy("f0").maxBy(2)
			.leftOuterJoin(exemplars).where("f0").equalTo("f0").with(new refineClusters());

	}

	// Init input messages
	private static class InitMessage implements MapFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> {

		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> output =
			new Tuple4<>(new LongValue(), new LongValue(), new DoubleValue(), new DoubleValue());

		@Override
		public Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>
		map(Tuple3<LongValue, LongValue, DoubleValue> in) {
			output.f0.setValue(in.f0.getValue());
			output.f1.setValue(in.f1.getValue());
			return output;
		}
	}

	// Create a list of a(i,K) + s(i,K) values joining similarities with messages
	@ForwardedFieldsFirst("f0; f1")
	@ForwardedFieldsSecond("f0; f1")
	private static class joinAvailabilitySimilarity
		implements JoinFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>,
			Tuple3<LongValue, LongValue, DoubleValue>> {

		private Tuple3<LongValue, LongValue, DoubleValue> output =
			new Tuple3<>(new LongValue(), new LongValue(), new DoubleValue());

		// Receives Tuple6<Trg, MaxValue, MaxNeighbour, SecondMaxValue, is> and Tuple3<src, target, similarity>
		// and returns a Tuple5<>
		@Override
		public Tuple3<LongValue, LongValue, DoubleValue>
		join(Tuple3<LongValue, LongValue, DoubleValue> similarity,
			Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> message) {

			output.f0.setValue(similarity.f0.getValue());
			output.f1.setValue(similarity.f1.getValue());
			output.f2.setValue(similarity.f2.getValue() + message.f3.getValue());

			return output;
		}
	}

	// Create a Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> reducing the 2 tuples with the max values
	@ForwardedFields("f1->f0")
	private static class responsibilityReduceGroup
		implements GroupReduceFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, DoubleValue, LongValue, DoubleValue>> {

		Tuple4<LongValue, DoubleValue, LongValue, DoubleValue> output = new Tuple4<>(new LongValue(), new DoubleValue(),
			new LongValue(), new DoubleValue());

		@Override
		public void reduce(Iterable<Tuple3<LongValue, LongValue, DoubleValue>> maxValues,
					Collector<Tuple4<LongValue, DoubleValue, LongValue, DoubleValue>> out) throws Exception {

			Long maxNeighbour = Long.valueOf(0);
			Long trg = Long.valueOf(0);
			double maxValue = 0;
			double secondMaxValue = 0;

			for (Tuple3<LongValue, LongValue, DoubleValue> val : maxValues) {

				if(val.f2.getValue() > maxValue){
					secondMaxValue = maxValue;
					maxValue = val.f2.getValue();
					maxNeighbour = val.f0.getValue();
					trg = val.f1.getValue();
				}else{
					secondMaxValue = val.f2.getValue();
				}
			}

			output.f0.setValue(trg);
			output.f1.setValue(maxValue);
			output.f2.setValue(maxNeighbour);
			output.f3.setValue(secondMaxValue);

			out.collect(output);

		}
	}

	// Subtract each responsibility
	@ForwardedFieldsFirst("f0")
	@ForwardedFieldsSecond("f0->f1; f1->f0")
	private static class responsibilityValue
		implements JoinFunction<Tuple4<LongValue, DoubleValue, LongValue, DoubleValue>,
		Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple3<LongValue, LongValue, DoubleValue>> {

		Tuple3<LongValue, LongValue, DoubleValue> output = new Tuple3<>(new LongValue(), new LongValue(),
			new DoubleValue());

		//Receives Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> and Tuple3<src, target, similarity>
		@Override
		public Tuple3<LongValue, LongValue, DoubleValue>
		join(Tuple4<LongValue, DoubleValue, LongValue, DoubleValue> maxValues,
			Tuple3<LongValue, LongValue, DoubleValue> similarity) {

			double responsibility;

			if(similarity.f0.getValue() == maxValues.f2.getValue()){
				responsibility = similarity.f2.getValue() - maxValues.f3.getValue();
			}else{
				responsibility = similarity.f2.getValue() - maxValues.f1.getValue();
			}

			output.f0.setValue(similarity.f1);
			output.f1.setValue(similarity.f0);
			output.f2.setValue(responsibility);

			return output;
		}
	}

	// Return a Tuple3<Trg, PositiveResponsibilitiesAccumulator, SelfResponsibility>
	@ForwardedFields("f1->f0")
	private static class availabilityReduceGroup
		implements GroupReduceFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple3<LongValue, DoubleValue, DoubleValue>> {

		Tuple3<LongValue, DoubleValue, DoubleValue> output = new Tuple3<>(new LongValue(), new DoubleValue(),
			new DoubleValue());

		@Override
		public void reduce(Iterable<Tuple3<LongValue, LongValue, DoubleValue>> responsibilities,
						Collector<Tuple3<LongValue, DoubleValue, DoubleValue>> out) throws Exception {

			double accum = 0;
			double selfResponsibility = 0;
			Long trg = Long.valueOf(0);

			for (Tuple3<LongValue, LongValue, DoubleValue> m : responsibilities) {
				if(m.f0.getValue() == m.f1.getValue()){
					selfResponsibility = m.f2.getValue();
					trg = m.f1.getValue();
				}else{
					if(m.f2.getValue() > 0){
						accum = accum + m.f2.getValue();
					}
				}
			}

			output.f0.setValue(trg);
			output.f1.setValue(accum);
			output.f2.setValue(selfResponsibility);

			out.collect(output);

		}
	}

	// Joins a Tuple3<Trg, PositiveResponsibilitiesAccumulator, SelfResponsibility> from previous step
	// and the responsibilities. For each responsibility will calculate the availability to be sent to the
	// responsibility source. In case of self availability will calculate the convergence too.
	@ForwardedFieldsFirst("f0")
	@ForwardedFieldsSecond("f0->f1; f1->f0")
	private static class availabilityValue
		implements JoinFunction<Tuple3<LongValue, DoubleValue, DoubleValue>,
		Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> {

		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> output = new Tuple4<>(new LongValue(), new LongValue(),
			new DoubleValue(), new DoubleValue());

		@Override
		public Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>
		join(Tuple3<LongValue, DoubleValue, DoubleValue> first,
			Tuple3<LongValue, LongValue, DoubleValue> responsibility) throws Exception {

			output.f0 = responsibility.f1;
			output.f1 = responsibility.f0;
			output.f2 = responsibility.f2;

			//For self availability calculate the convergence
			if(responsibility.f1.getValue() == responsibility.f0.getValue()){
				output.f3 = new DoubleValue(first.f1.getValue());
			}else{
				//Take the responsibility value in case is positive, it will be subtracted to the positive accumulator later
				if(responsibility.f2.getValue() > 0) {
					output.f3 = new DoubleValue(Math.min(0, first.f1.getValue() - responsibility.f2.getValue() + first.f2.getValue()));
				}else{
					output.f3 = new DoubleValue(Math.min(0, first.f1.getValue() + first.f2.getValue()));
				}

			}

			return output;
		}
	}

	@ForwardedFieldsFirst("f0; f1")
	@ForwardedFieldsSecond("f0->f1; f1->f0")
	private static class dampedRValue
		extends RichJoinFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>, Tuple3<LongValue, LongValue, DoubleValue>> {

		private double damping;
		private double threshold;

		Tuple3<LongValue, LongValue, DoubleValue> output = new Tuple3<>(new LongValue(), new LongValue(), new DoubleValue());

		dampedRValue(double damping, double threshold){
			this.damping = damping;
			this.threshold = threshold;
		}

		@Override
		public Tuple3<LongValue, LongValue, DoubleValue>
		join(Tuple3<LongValue, LongValue, DoubleValue> newValue,
			Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> oldValue) {

			boolean converged;

			output.f0.setValue(newValue.f0.getValue());
			output.f1.setValue(newValue.f1.getValue());

			output.f2.setValue((1 - damping) * newValue.f2.getValue() + damping * oldValue.f2.getValue());
			converged = Math.abs(newValue.f2.getValue() - oldValue.f2.getValue()) < threshold;

			LongSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(CONVERGENCE_MESSAGES);
			if(converged){
				agg.aggregate(1);
			}

			return output;
		}
	}

	@ForwardedFieldsFirst("f0; f1")
	@ForwardedFieldsSecond("f0->f0; f1->f1")
	private static class dampedAValue
		extends RichJoinFunction<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> {

		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> output =
			new Tuple4<>(new LongValue(), new LongValue(), new DoubleValue(), new DoubleValue());

		private double damping;
		private double threshold;

		dampedAValue(double damping, double threshold){
			this.damping = damping;
			this.threshold = threshold;
		}

		@Override
		public Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>
		join(Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> newValue,
			Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> oldValue) {

			boolean converged;

			output.f0.setValue(newValue.f0.getValue());
			output.f1.setValue(newValue.f1.getValue());
			output.f2.setValue(newValue.f2.getValue());

			output.f3.setValue((1 - damping) * newValue.f3.getValue() + damping * oldValue.f3.getValue());
			converged = Math.abs(newValue.f3.getValue() - oldValue.f3.getValue()) < threshold;

			LongSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(CONVERGENCE_MESSAGES);
			if(converged){
				agg.aggregate(1);
			}

			return output;
		}
	}

	private static class MessageConvergence
		implements ConvergenceCriterion<LongValue> {
		private long convergenceThreshold;

		public MessageConvergence(long convergenceThreshold) {
			this.convergenceThreshold = convergenceThreshold;
		}

		@Override
		public boolean isConverged(int iteration, LongValue value) {
			long val = value.getValue();
			return (val == convergenceThreshold);
		}
	}

	public static final class FilterExemplars
		implements FilterFunction<Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>> {
		public boolean filter(Tuple4<LongValue, LongValue, DoubleValue,
			DoubleValue> value) throws Exception {
			return (value.f0.equals(value.f1)) && (value.f2.getValue() +
				value.f3.getValue() > 0);
		}
	}

	private static class refineClusters
		implements JoinFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, LongValue, DoubleValue, DoubleValue>,
		Tuple3<LongValue, LongValue, DoubleValue>>{

		Tuple3<LongValue, LongValue, DoubleValue> output =
			new Tuple3<>(new LongValue(), new LongValue(), new DoubleValue());

		@Override
		public 	Tuple3<LongValue, LongValue, DoubleValue> join(Tuple3<LongValue, LongValue, DoubleValue> first,
				Tuple4<LongValue, LongValue, DoubleValue, DoubleValue> second) throws Exception {
			if(second != null) {
				output.f0.setValue(first.f0.getValue());
				output.f1.setValue(first.f0.getValue());
				output.f2.setValue(first.f2.getValue());
			}else{
				output.f0.setValue(first.f0.getValue());
				output.f1.setValue(first.f1.getValue());
				output.f2.setValue(first.f2.getValue());
			}

			return output;
		}
	}

}
