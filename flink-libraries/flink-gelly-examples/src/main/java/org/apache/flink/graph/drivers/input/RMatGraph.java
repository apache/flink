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

package org.apache.flink.graph.drivers.input;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToStringValue;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.graph.drivers.parameter.DoubleParameter;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.graph.drivers.parameter.Simplify;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Generate an {@code RMatGraph} with {@link IntValue}, {@link LongValue},
 * or {@link StringValue} keys.
 *
 * @see org.apache.flink.graph.generator.RMatGraph
 *
 * @param <K> key type
 */
public class RMatGraph<K extends Comparable<K>>
extends ParameterizedBase
implements Input<K, NullValue, NullValue> {

	private static final String INTEGER = "integer";

	private static final String LONG = "long";

	private static final String STRING = "string";

	private ChoiceParameter type = new ChoiceParameter(this, "type")
		.setDefaultValue(INTEGER)
		.addChoices(LONG, STRING);

	// generate graph with 2^scale vertices
	private LongParameter scale = new LongParameter(this, "scale")
		.setDefaultValue(10)
		.setMinimumValue(1);

	// generate graph with edgeFactor * 2^scale edges
	private LongParameter edgeFactor = new LongParameter(this, "edge_factor")
		.setDefaultValue(16)
		.setMinimumValue(1);

	// matrix parameters "a", "b", "c", and implicitly "d = 1 - a - b - c"
	// describe the skew in the recursive matrix
	private DoubleParameter a = new DoubleParameter(this, "a")
		.setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_A)
		.setMinimumValue(0.0, false);

	private DoubleParameter b = new DoubleParameter(this, "b")
		.setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_B)
		.setMinimumValue(0.0, false);

	private DoubleParameter c = new DoubleParameter(this, "c")
		.setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_C)
		.setMinimumValue(0.0, false);

	// noise randomly pertubates the matrix parameters for each successive bit
	// for each generated edge
	private BooleanParameter noiseEnabled = new BooleanParameter(this, "noise_enabled");

	private DoubleParameter noise = new DoubleParameter(this, "noise")
		.setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_NOISE)
		.setMinimumValue(0.0, true)
		.setMaximumValue(2.0, true);

	private Simplify simplify = new Simplify(this);

	private LongParameter littleParallelism = new LongParameter(this, "little_parallelism")
		.setDefaultValue(PARALLELISM_DEFAULT);

	@Override
	public String getName() {
		return RMatGraph.class.getSimpleName();
	}

	@Override
	public String getIdentity() {
		return getName() + WordUtils.capitalize(type.getValue()) +
			" (s" + scale + "e" + edgeFactor + simplify.getShortString() + ")";
	}

	/**
	 * Generate the graph as configured.
	 *
	 * @param env Flink execution environment
	 * @return input graph
	 */
	public Graph<K, NullValue, NullValue> create(ExecutionEnvironment env) throws Exception {
		int lp = littleParallelism.getValue().intValue();

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		long vertexCount = 1L << scale.getValue();
		long edgeCount = vertexCount * edgeFactor.getValue();

		Graph<LongValue, NullValue, NullValue> rmatGraph = new org.apache.flink.graph.generator.RMatGraph<>(
				env, rnd, vertexCount, edgeCount)
			.setConstants(a.getValue().floatValue(), b.getValue().floatValue(), c.getValue().floatValue())
			.setNoise(noiseEnabled.getValue(), noise.getValue().floatValue())
			.setParallelism(lp)
			.generate();

		Graph<K, NullValue, NullValue> graph;

		switch (type.getValue()) {
			case INTEGER:
				if (scale.getValue() > 32) {
					throw new ProgramParametrizationException(
						"Scale '" + scale.getValue() + "' must be no greater than 32 for type 'integer'");
				}
				graph = (Graph<K, NullValue, NullValue>) rmatGraph
					.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue()));
				break;

			case LONG:
				if (scale.getValue() > 64) {
					throw new ProgramParametrizationException(
						"Scale '" + scale.getValue() + "' must be no greater than 64 for type 'long'");
				}
				graph = (Graph<K, NullValue, NullValue>) rmatGraph;
				break;

			case STRING:
				// scale bound is same as LONG since keys are generated as LongValue
				if (scale.getValue() > 64) {
					throw new ProgramParametrizationException(
						"Scale '" + scale.getValue() + "' must be no greater than 64 for type 'string'");
				}
				graph = (Graph<K, NullValue, NullValue>) rmatGraph
					.run(new TranslateGraphIds<LongValue, StringValue, NullValue, NullValue>(new LongValueToStringValue()));
				break;

			default:
				throw new ProgramParametrizationException("Unknown type '" + type.getValue() + "'");
		}

		// simplify *after* the translation from LongValue to IntValue or
		// StringValue to improve the performance of the simplify operators
		return simplify.simplify(graph);
	}
}
