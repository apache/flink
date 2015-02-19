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

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.windowing.StreamDiscretizer;

public class WindowingOptimzier {

	public static void optimizeGraph(StreamGraph streamGraph) {
		Set<Entry<String, StreamInvokable<?, ?>>> invokables = streamGraph.getInvokables();
		List<Tuple2<String, StreamDiscretizer<?>>> discretizers = new ArrayList<Tuple2<String, StreamDiscretizer<?>>>();

		// Get the discretizers
		for (Entry<String, StreamInvokable<?, ?>> entry : invokables) {
			if (entry.getValue() instanceof StreamDiscretizer) {
				discretizers.add(new Tuple2<String, StreamDiscretizer<?>>(entry.getKey(),
						(StreamDiscretizer<?>) entry.getValue()));
			}
		}

		// Share common discrtizers
		setDiscretizerReuse(streamGraph, discretizers);
	}

	private static void setDiscretizerReuse(StreamGraph streamGraph,
			List<Tuple2<String, StreamDiscretizer<?>>> discretizers) {
		List<Tuple2<StreamDiscretizer<?>, List<String>>> matchingDiscretizers = new ArrayList<Tuple2<StreamDiscretizer<?>, List<String>>>();

		for (Tuple2<String, StreamDiscretizer<?>> discretizer : discretizers) {
			boolean inMatching = false;
			for (Tuple2<StreamDiscretizer<?>, List<String>> matching : matchingDiscretizers) {
				Set<String> discretizerInEdges = new HashSet<String>(
						streamGraph.getInEdges(discretizer.f0));
				Set<String> matchingInEdges = new HashSet<String>(
						streamGraph.getInEdges(matching.f1.get(0)));

				if (discretizer.f1.equals(matching.f0)
						&& discretizerInEdges.equals(matchingInEdges)) {
					matching.f1.add(discretizer.f0);
					inMatching = true;
					break;
				}
			}
			if (!inMatching) {
				List<String> matchingNames = new ArrayList<String>();
				matchingNames.add(discretizer.f0);
				matchingDiscretizers.add(new Tuple2<StreamDiscretizer<?>, List<String>>(
						discretizer.f1, matchingNames));
			}
		}

		for (Tuple2<StreamDiscretizer<?>, List<String>> matching : matchingDiscretizers) {
			List<String> matchList = matching.f1;
			if (matchList.size() > 1) {
				String first = matchList.get(0);
				for (int i = 1; i < matchList.size(); i++) {
					replaceDiscretizer(streamGraph, matchList.get(i), first);
				}
			}
		}
	}

	private static void replaceDiscretizer(StreamGraph streamGraph, String toReplace,
			String replaceWith) {
		// Convert to array to create a copy
		List<String> outEdges = new ArrayList<String>(streamGraph.getOutEdges(toReplace));

		int numOutputs = outEdges.size();

		// Reconnect outputs
		for (int i = 0; i < numOutputs; i++) {
			String outName = outEdges.get(i);

			streamGraph.setEdge(replaceWith, outName,
					streamGraph.getOutPartitioner(toReplace, outName), 0, new ArrayList<String>());
			streamGraph.removeEdge(toReplace, outName);
		}

		List<String> inEdges = new ArrayList<String>(streamGraph.getInEdges(toReplace));
		// Remove inputs
		for (String inName : inEdges) {
			streamGraph.removeEdge(inName, toReplace);
		}

		streamGraph.removeVertex(toReplace);
	}
}
