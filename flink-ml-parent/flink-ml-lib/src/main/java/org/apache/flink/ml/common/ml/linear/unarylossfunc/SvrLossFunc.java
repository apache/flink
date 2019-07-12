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
 * SVR (Support Vector Regression) loss function.
 */
public class SvrLossFunc implements UnaryLossFunc {

	private double epsilon;

	public SvrLossFunc(double epsilon) {
		if (epsilon < 0) {
			throw new IllegalArgumentException("Parameter epsilon can not be negtive.");
		}
		this.epsilon = epsilon;
	}

	@Override
	public double loss(double eta, double y) {
		return Math.max(0, Math.abs(eta - y) - epsilon);
	}

	@Override
	public double derivative(double eta, double y) {
		if (Math.abs(eta - y) > epsilon) {
			return Math.signum(eta - y);
		} else {
			return 0;
		}
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return 0;
	}

}
