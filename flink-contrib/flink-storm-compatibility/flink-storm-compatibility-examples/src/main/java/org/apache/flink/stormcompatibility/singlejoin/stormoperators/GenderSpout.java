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
package org.apache.flink.stormcompatibility.singlejoin.stormoperators;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.flink.stormcompatibility.util.AbstractStormSpout;

public class GenderSpout extends AbstractStormSpout {
	private static final long serialVersionUID = -5079110197950743927L;

	private int counter = 9;
	private Fields outFields;

	public GenderSpout(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void nextTuple() {
		if (counter >= 0) {
			this.collector.emit(new Values(counter, counter + 20));
			counter--;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}
}
