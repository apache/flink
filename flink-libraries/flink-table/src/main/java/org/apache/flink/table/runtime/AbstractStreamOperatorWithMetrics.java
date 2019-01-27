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

package org.apache.flink.table.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * AbstractStreamOperatorWithMetrics wraps the original output of the  AbstractStreamOperator in a
 * special output to collect metrics of operator in runtime, and accumulates the metrics when close
 * operator.
 *
 * @param <OUT> The output type of the operator.
 */
public class AbstractStreamOperatorWithMetrics<OUT> extends AbstractStreamOperator<OUT> {

	public static final String ACCUMULATOR_PREFIX = "OperatorMetric_";

	public static final String ROW_COUNT_METRICS = "rowCount";

	public static final String METRICS_CONF_KEY = "operatorMetricCollect";

	public static final String METRICS_CONF_VALUE = "true";

	private Counter counter;

	protected boolean closed = false;

	private transient Configuration sqlConf;

	public AbstractStreamOperatorWithMetrics() {
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	// id of the rel who use this operator.
	private int relID;

	@Override
	public void open() throws Exception {
		super.open();
		Output<StreamRecord<OUT>> rawOutput = this.output;
		if (isCollectMetricEnabled()) {
			counter = getMetricGroup().counter(ROW_COUNT_METRICS);
			this.output = new Output<StreamRecord<OUT>>() {

				@Override
				public void collect(StreamRecord<OUT> record) {
					// TODO: 1. suppose one record is one row here , does not take
					// VectorizedColumnBatchTypeInfo into consideration yet.
					// TODO: 2. only collect row counter here, does not collect other stats yet.
					counter.inc();
					rawOutput.collect(record);
				}

				@Override
				public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
					counter.inc();
					rawOutput.collect(outputTag, record);
				}

				@Override
				public void close() {
					rawOutput.close();
				}

				@Override
				public void emitWatermark(Watermark mark) {
					rawOutput.emitWatermark(mark);
				}

				@Override
				public void emitLatencyMarker(LatencyMarker latencyMarker) {
					rawOutput.emitLatencyMarker(latencyMarker);
				}
			};
		}
	}

	@Override
	public void close() throws Exception {
		if (isCollectMetricEnabled()) {
			LongCounter rowCountAcc = new LongCounter(counter.getCount());
			String rowCountAccName = ACCUMULATOR_PREFIX + getOperatorConfig().getVertexID();
			getRuntimeContext().addAccumulator(rowCountAccName, rowCountAcc);
		}
		super.close();
		closed = true;
	}

	@Override
	public void dispose() throws Exception {
		if (!closed) {
			close();
		}
		super.dispose();
	}

	private boolean isCollectMetricEnabled() {
		ExecutionConfig.GlobalJobParameters parameters = getExecutionConfig()
				.getGlobalJobParameters();
		if (parameters != null) {
			String metricsConfig = parameters.toMap().get(METRICS_CONF_KEY);
			return metricsConfig != null && metricsConfig
					.equalsIgnoreCase(METRICS_CONF_VALUE);
		} else {
			return false;
		}
	}

	public Configuration getSqlConf() {
		if (sqlConf != null) {
			return sqlConf;
		}
		Configuration conf = getContainingTask().getJobConfiguration();
		ExecutionConfig.GlobalJobParameters paras = getExecutionConfig().getGlobalJobParameters();
		if (paras != null) {
			conf.addAll(paras.toMap());
		}
		this.sqlConf = conf;
		return conf;
	}
}
