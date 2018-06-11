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

package org.apache.flink.streaming.api.functions.test;

import org.apache.flink.api.common.functions.util.test.SimpleValueState;
import org.apache.flink.api.common.functions.util.test.TestingRuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for the {@link TestingRuntimeContext}.
 */
@SuppressWarnings("unchecked")
public class TestingRuntimeContextTest {

	@Test
	public void testEnrichmentFunction() throws Exception {
		TestingRuntimeContext ctx = new TestingRuntimeContext(true);
		EnrichmentFunction func = new EnrichmentFunction();
		func.setRuntimeContext(ctx);

		CoProcessFunction.Context context = mock(EnrichmentFunction.Context.class);
		CoProcessFunction.OnTimerContext timerContext = mock(EnrichmentFunction.OnTimerContext.class);
		TimerService timerService = mock(TimerService.class);
		doAnswer(invocationOnMock -> {
			OutputTag outputTag = invocationOnMock.getArgumentAt(0, OutputTag.class);
			Object value = invocationOnMock.getArgumentAt(1, Object.class);
			ctx.addSideOutput(outputTag, value);
			return null;
		}).when(timerContext).output(any(OutputTag.class), any());
		doReturn(timerService).when(context).timerService();
		doNothing().when(timerService).registerEventTimeTimer(anyLong());

		ValueStateDescriptor<TaxiRide> rideStateDesc = new ValueStateDescriptor<>("saved ride", TaxiRide.class);
		ValueStateDescriptor<TaxiFare> fareStateDesc = new ValueStateDescriptor<>("saved fare", TaxiFare.class);
		ctx.setState(rideStateDesc, new SimpleValueState<>(null));
		ctx.setState(fareStateDesc, new SimpleValueState<TaxiFare>(null));
		func.open(new Configuration());

		TaxiRide ride1 = new TaxiRide(1);
		func.processElement1(ride1, context, ctx.getCollector());
		Assert.assertEquals(ctx.getState(rideStateDesc).value(), ride1);

		TaxiFare fare1 = new TaxiFare(1);
		func.processElement2(fare1, context, ctx.getCollector());
		Assert.assertEquals(ctx.getState(rideStateDesc).value(), null);
		Assert.assertEquals(ctx.getCollectorOutput(), Collections.singletonList(new Tuple2(ride1, fare1)));

		TaxiFare fare2 = new TaxiFare(2);
		func.processElement2(fare2, context, ctx.getCollector());
		Assert.assertEquals(ctx.getState(fareStateDesc).value(), fare2);

		func.onTimer(0L, timerContext, ctx.getCollector());
		Assert.assertEquals(Collections.singletonList(fare2), ctx.getSideOutput(unmatchedFares));

	}


	static OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	static class TaxiRide {
		private long eventTime;

		TaxiRide(long eventTime) {
			this.eventTime = eventTime;
		}

		Long getEventTime() {
			return eventTime;
		}

	}

	static class TaxiFare {
		private long eventTime;

		TaxiFare(long eventTime) {
			this.eventTime = eventTime;
		}

		Long getEventTime() {
			return eventTime;
		}

	}

	/**
	 * User-defined function that joins rides and fare.
	 */
	public static class EnrichmentFunction extends CoProcessFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		// keyed, managed state
		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration config) {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			if (fareState.value() != null) {
				ctx.output(unmatchedFares, fareState.value());
				fareState.clear();
			}
			if (rideState.value() != null) {
				ctx.output(unmatchedRides, rideState.value());
				rideState.clear();
			}
		}

		@Override
		public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareState.value();
			if (fare != null) {
				fareState.clear();
				out.collect(new Tuple2(ride, fare));
			} else {
				rideState.update(ride);
				// as soon as the watermark arrives, we can stop waiting for the corresponding fare
				context.timerService().registerEventTimeTimer(ride.getEventTime());
			}
		}

		@Override
		public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideState.value();
			if (ride != null) {
				rideState.clear();
				out.collect(new Tuple2(ride, fare));
			} else {
				fareState.update(fare);
				// wait up to 6 hours for the corresponding ride END event, then clear the state
				context.timerService().registerEventTimeTimer(fare.getEventTime() + 6 * 60 * 60 * 1000);
			}
		}
	}

}
