/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;

/**
 * The type Abstract two input processor.
 */
class TwoInputWatermarkProcessor {

	/**
	 * The Input 1 watermark gauge.
	 */
	private final WatermarkGauge input1WatermarkGauge = new WatermarkGauge();
	/**
	 * The Input 2 watermark gauge.
	 */
	private final WatermarkGauge input2WatermarkGauge = new WatermarkGauge();

	private final MinWatermarkGauge
		minInputWatermarkGauge = new MinWatermarkGauge(input1WatermarkGauge, input2WatermarkGauge);

	/**
	 * Instantiates a new Abstract two input processor.
	 *
	 * @param operator                  the operator
	 * @param minAllInputWatermarkGauge the min all input watermark gauge
	 */
	TwoInputWatermarkProcessor(
		TwoInputStreamOperator operator,
		MinWatermarkGauge minAllInputWatermarkGauge) {

		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);

		minAllInputWatermarkGauge.addWatermarkGauge(input1WatermarkGauge);
		minAllInputWatermarkGauge.addWatermarkGauge(input2WatermarkGauge);
	}

	WatermarkGauge getInput1WatermarkGauge() {
		return input1WatermarkGauge;
	}

	WatermarkGauge getInput2WatermarkGauge() {
		return input2WatermarkGauge;
	}
}

