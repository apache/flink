/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi;

import org.apache.flink.annotation.Public;
import org.apache.flink.contrib.siddhi.operator.SiddhiStream;
import org.apache.flink.contrib.siddhi.utils.SiddhiOperatorUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Siddhi CEP Stream API
 */
@Public
public class SiddhiEnvironment {
	private Map<String,Class<?>> extensions;

	private static final SiddhiEnvironment INSTANCE = new SiddhiEnvironment();
	private SiddhiEnvironment(){
		
	}

	public static SiddhiEnvironment getInstance(){
		return INSTANCE;
	}

	public static void registerExtension(String extensionName,Class<?> extensionClass){
		getInstance().extensions.put(extensionName,extensionClass);
	}

	public static <T> SiddhiPlanBuilder connect(String inStreamId, DataStream<T> inStream, String ... fieldNames){
		return new SiddhiPlanBuilder().register(inStreamId,inStream,fieldNames);
	}

	public static class SiddhiPlanBuilder {
		private SiddhiStream plan;
		public SiddhiPlanBuilder(){
			plan = new SiddhiStream();
		}

		private <T> SiddhiPlanBuilder register(String streamId, DataStream<T> inStream, String ... fieldNames){
			plan.registerInput(streamId,inStream,fieldNames);
			return this;
		}

		public SiddhiOutputBuilder sql(String executionPlan){
			Preconditions.checkNotNull(executionPlan,"Execution plan is null");
			plan.setExecutionExpression(executionPlan);
			return new SiddhiOutputBuilder(plan);
		}
	}

	public static class SiddhiOutputBuilder {
		private final SiddhiStream plan;
		public SiddhiOutputBuilder(SiddhiStream plan) {
			this.plan = plan;
		}

		@SuppressWarnings("unchecked")
		public DataStream<Map<String,Object>> returns(String outStreamId){
			this.plan.setOutput(outStreamId,Map.class);
			return SiddhiOperatorUtils.createDataStream(this.plan);
		}

		@SuppressWarnings("unchecked")
		public <T> DataStream<T> returns(String outStreamId, Class<T> outType){
			this.plan.setOutput(outStreamId,outType);
			return SiddhiOperatorUtils.createDataStream(this.plan);
		}
	}
}
