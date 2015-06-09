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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("deprecation")
public class SingleJoinBolt implements IRichBolt {
	OutputCollector collector;
	Fields idFields;
	Fields outFields;
	int numSources = 2;
	TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>> pending;
	Map<String, GlobalStreamId> fieldLocations;

	public SingleJoinBolt(Fields outFields) {
		this.outFields = outFields;
	}

	@SuppressWarnings({"rawtypes", "null"})
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		fieldLocations = new HashMap<String, GlobalStreamId>();
		this.collector = collector;
		int timeout = 100;
		pending = new TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>>(timeout, new ExpireCallback());
		// numSources = context.getThisSources().size();
		Set<String> idFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			Fields fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());
			if (idFields == null) {
				idFields = setFields;
			} else {
				idFields.retainAll(setFields);
			}

			for (String outfield : outFields) {
				for (String sourcefield : fields) {
					if (outfield.equals(sourcefield)) {
						fieldLocations.put(outfield, source);
					}
				}
			}
		}
		this.idFields = new Fields(new ArrayList<String>(idFields));

		if (fieldLocations.size() != outFields.size()) {
			throw new RuntimeException("Cannot find all outfields among sources");
		}
	}

	@Override
	public void execute(Tuple tuple) {
		List<Object> id = tuple.select(idFields);
		GlobalStreamId streamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());
		if (!pending.containsKey(id)) {
			pending.put(id, new HashMap<GlobalStreamId, Tuple>());
		}
		Map<GlobalStreamId, Tuple> parts = pending.get(id);
		if (parts.containsKey(streamId)) {
			throw new RuntimeException("Received same side of single join twice");
		}
		parts.put(streamId, tuple);
		if (parts.size() == numSources) {
			pending.remove(id);
			List<Object> joinResult = new ArrayList<Object>();
			for (String outField : outFields) {
				GlobalStreamId loc = fieldLocations.get(outField);
				joinResult.add(parts.get(loc).getValueByField(outField));
			}
			collector.emit(new ArrayList<Tuple>(parts.values()), joinResult);

			for (Tuple part : parts.values()) {
				collector.ack(part);
			}
		}
	}

	@Override
	public void cleanup() {
		/* nothing to do */
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	private class ExpireCallback implements TimeCacheMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {
		@Override
		public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {
			for (Tuple tuple : tuples.values()) {
				collector.fail(tuple);
			}
		}
	}
}
