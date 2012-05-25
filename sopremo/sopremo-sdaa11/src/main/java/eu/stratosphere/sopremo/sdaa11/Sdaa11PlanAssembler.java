/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.sdaa11;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.sdaa11.clustering.SimpleClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;

/**
 * @author skruse
 * 
 */
public class Sdaa11PlanAssembler implements PlanAssembler {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String
	 * [])
	 */
	@Override
	public Plan getPlan(final String... args) {

		System.out.println(Arrays.toString(args));

		Plan pactPlan;
//		pactPlan = getSimpleClusteringPlan(args);
		pactPlan = this.getInitialClusteringPlan(args);
		return pactPlan;
	}

	/**
	 * @param args
	 * @return
	 */
	private Plan getInitialClusteringPlan(final String[] args) {
		final Source sampleSource = new Source(args[0]);

		final InitialClustering initialClustering = new InitialClustering()
				.withInputs(sampleSource);

		final List<Sink> sinks = new ArrayList<Sink>();
		int i = 1;
		for (final JsonStream output : initialClustering.getOutputs()) {
			final Sink sink = new Sink(args[i++]).withInputs(output);
			sinks.add(sink);
		}

		final SopremoPlan sopremoPlan = new SopremoPlan();
		sopremoPlan.setSinks(sinks);
		final Plan pactPlan = sopremoPlan.asPactPlan();
		SopremoUtil.serialize(pactPlan.getPlanConfiguration(),
				SopremoUtil.CONTEXT, new EvaluationContext());
		return pactPlan;

	}

	private Plan getSimpleClusteringPlan(final String... args) {
		final Source sampleSource = new Source(args[0]);
		final Source remainSource = new Source(args[1]);

		final SimpleClustering simpleClustering = new SimpleClustering();
		simpleClustering.withInputs(sampleSource, remainSource);

		final List<Sink> sinks = new ArrayList<Sink>();
		int i = 2;
		for (final JsonStream output : simpleClustering.getOutputs()) {
			final Sink sink = new Sink(args[i++]).withInputs(output);
			sinks.add(sink);
		}

		final SopremoPlan sopremoPlan = new SopremoPlan();
		sopremoPlan.setSinks(sinks);
		final Plan pactPlan = sopremoPlan.asPactPlan();
		SopremoUtil.serialize(pactPlan.getPlanConfiguration(),
				SopremoUtil.CONTEXT, new EvaluationContext());

		return pactPlan;
	}

}
