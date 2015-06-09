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

public class AgeSpout extends AbstractStormSpout {
	private static final long serialVersionUID = -4008858647468647019L;

	private int counter = 0;
	private String gender;
	private Fields outFields;

	public AgeSpout(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void nextTuple() {
		if (this.counter < 10) {
			if (counter % 2 == 0) {
				gender = "male";
			} else {
				gender = "female";
			}
			this.collector.emit(new Values(counter, gender));
			counter++;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}

}
