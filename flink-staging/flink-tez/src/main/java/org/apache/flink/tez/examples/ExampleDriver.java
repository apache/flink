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

package org.apache.flink.tez.examples;

import org.apache.hadoop.util.ProgramDriver;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.Set;

public class ExampleDriver {

	private static final DecimalFormat formatter = new DecimalFormat("###.##%");

	public static void main(String [] args){
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("wc", WordCount.class,
					"Wordcount");
			pgd.addClass("tpch3", TPCHQuery3.class,
					"Modified TPC-H 3 query");
			pgd.addClass("tc", TransitiveClosureNaiveStep.class,
					"One step of transitive closure");
			pgd.addClass("pr", PageRankBasicStep.class,
					"One step of PageRank");
			pgd.addClass("cc", ConnectedComponentsStep.class,
					"One step of connected components");
			exitCode = pgd.run(args);
		} catch(Throwable e){
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

	public static void printDAGStatus(DAGClient dagClient, String[] vertexNames)
			throws IOException, TezException {
		printDAGStatus(dagClient, vertexNames, false, false);
	}

	public static void printDAGStatus(DAGClient dagClient, String[] vertexNames, boolean displayDAGCounters, boolean displayVertexCounters)
			throws IOException, TezException {
		Set<StatusGetOpts> opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
		DAGStatus dagStatus = dagClient.getDAGStatus(
				(displayDAGCounters ? opts : null));
		Progress progress = dagStatus.getDAGProgress();
		double vProgressFloat = 0.0f;
		if (progress != null) {
			System.out.println("");
			System.out.println("DAG: State: "
					+ dagStatus.getState()
					+ " Progress: "
					+ (progress.getTotalTaskCount() < 0 ? formatter.format(0.0f) :
					formatter.format((double)(progress.getSucceededTaskCount())
							/progress.getTotalTaskCount())));
			for (String vertexName : vertexNames) {
				VertexStatus vStatus = dagClient.getVertexStatus(vertexName,
						(displayVertexCounters ? opts : null));
				if (vStatus == null) {
					System.out.println("Could not retrieve status for vertex: "
							+ vertexName);
					continue;
				}
				Progress vProgress = vStatus.getProgress();
				if (vProgress != null) {
					vProgressFloat = 0.0f;
					if (vProgress.getTotalTaskCount() == 0) {
						vProgressFloat = 1.0f;
					} else if (vProgress.getTotalTaskCount() > 0) {
						vProgressFloat = (double)vProgress.getSucceededTaskCount()
								/vProgress.getTotalTaskCount();
					}
					System.out.println("VertexStatus:"
							+ " VertexName: "
							+ (vertexName.equals("ivertex1") ? "intermediate-reducer"
							: vertexName)
							+ " Progress: " + formatter.format(vProgressFloat));
				}
				if (displayVertexCounters) {
					TezCounters counters = vStatus.getVertexCounters();
					if (counters != null) {
						System.out.println("Vertex Counters for " + vertexName + ": "
								+ counters);
					}
				}
			}
		}
		if (displayDAGCounters) {
			TezCounters counters = dagStatus.getDAGCounters();
			if (counters != null) {
				System.out.println("DAG Counters: " + counters);
			}
		}
	}
}
