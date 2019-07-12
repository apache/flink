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

/**
 * Huber loss function.
 * https://en.wikipedia.org/wiki/Huber_loss
 */
public class HuberLossFunc implements UnaryLossFunc {
	private double delta;

	public HuberLossFunc(double delta) {
		if (delta <= 0) {
			throw new IllegalArgumentException("Parameter delta must be positive.");
		}
		this.delta = delta;
	}

	@Override
	public double loss(double eta, double y) {
		double x = Math.abs(eta - y);
		return x > delta ? delta * (x - delta / 2) : x * x / 2;
	}

	@Override
	public double derivative(double eta, double y) {
		double x = eta - y;
		return Math.abs(x) > delta ? Math.signum(x) * delta : x;
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return Math.abs(eta - y) > delta ? 0.0 : 1.0;
	}
}
