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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.apache.flink.streaming.api.graph.StreamGraphGeneratorTest.unsupportedOperatorFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link MultipleInputTransformation} test.
 */
public class MultipleInputTransformationTest {

	@Test
	public void testNaming() {
		Random random = new Random();
		MultipleInputTransformation<Long> transformation =
			new MultipleInputTransformation<>(getString(random), unsupportedOperatorFactory(), TypeInformation.of(Long.class), 1);
		for (int i = 0; i < 10; i++) {
			transformation.addInput(new SourceTransformation<>(getString(random),
				new NumberSequenceSource(0, 1),
				WatermarkStrategy.noWatermarks(),
				TypeInformation.of(Long.class),
				1));
		}
		transformation.getInputs().forEach(input -> assertTrue(transformation.getName().contains(input.getName())));
	}

	@Test
	public void testNamingWithNoInputs() {
		Random random = new Random();
		String name = getString(random);
		MultipleInputTransformation<Long> transformation =
			new MultipleInputTransformation<>(name, unsupportedOperatorFactory(), TypeInformation.of(Long.class), 1);
		assertEquals(transformation.getName(), name);
	}

	private String getString(Random random) {
		byte[] buf = new byte[15];
		random.nextBytes(buf);
		return new String(buf, StandardCharsets.UTF_8);
	}

}
