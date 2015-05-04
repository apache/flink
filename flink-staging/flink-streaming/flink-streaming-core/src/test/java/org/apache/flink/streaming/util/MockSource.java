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

package org.apache.flink.streaming.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class MockSource<T> {

	public static <T> List<T> createAndExecute(SourceFunction<T> sourceFunction) throws Exception {
		List<T> outputs = new ArrayList<T>();
		if (sourceFunction instanceof RichSourceFunction) {
			((RichSourceFunction<T>) sourceFunction).open(new Configuration());
		}
		try {
			Collector<T> collector = new MockOutput<T>(outputs);
			while (!sourceFunction.reachedEnd()) {
				collector.collect(sourceFunction.next());
			}
		} catch (Exception e) {
			throw new RuntimeException("Cannot invoke source.", e);
		}
		return outputs;
	}
}
