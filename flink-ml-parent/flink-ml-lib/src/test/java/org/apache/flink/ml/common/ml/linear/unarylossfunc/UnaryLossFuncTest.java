/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.ml.linear.unarylossfunc;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for the unary loss functions.
 */
public class UnaryLossFuncTest {
	@Test
	public void test() throws Exception {
		UnaryLossFunc[] lossFuncs;

		lossFuncs = new UnaryLossFunc[] {
			new ExponentialLossFunc(),
			new HingeLossFunc(),
			new LogisticLossFunc(),
			new LogLossFunc(),
			new PerceptronLossFunc(),
			new ZeroOneLossFunc()
		};

		for (UnaryLossFunc lossFunc : lossFuncs) {
			assertTrue(lossFunc.loss(-1.0, 1.0) > 1.0 - 1e-10);
			assertTrue(lossFunc.loss(1.0, 1.0) < 0.5);
			assertTrue(lossFunc.loss(-0.5, 1.0) - lossFunc.loss(0.5, 1.0) > 0.49);
			assertTrue(lossFunc.loss(-0.5, 1.0) == lossFunc.loss(0.5, -1.0));
			assertTrue(lossFunc.derivative(-0.5, 1.0) <= lossFunc.derivative(0.5, 1.0));
			assertTrue(lossFunc.secondDerivative(-0.5, 1.0) >= lossFunc.secondDerivative(0.5, 1.0));
		}

		lossFuncs = new UnaryLossFunc[] {
			new SquareLossFunc(),
			new SvrLossFunc(1.0),
			new HuberLossFunc(1.0)
		};

		for (UnaryLossFunc lossFunc : lossFuncs) {
			assertTrue(Math.abs(lossFunc.loss(0.0, 0.0)) < 1e-10);
			assertTrue(Math.abs(lossFunc.derivative(0.0, 0.0)) < 1e-10);
			assertTrue(lossFunc.derivative(-0.5, 0.0) == -lossFunc.derivative(0.5, 0.0));
			assertTrue(lossFunc.secondDerivative(-0.5, 0.0) == lossFunc.secondDerivative(0.5, 0.0));
		}
	}
}
