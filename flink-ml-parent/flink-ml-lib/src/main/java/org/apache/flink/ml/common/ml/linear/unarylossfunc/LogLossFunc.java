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
 * Log loss function.
 * https://en.wikipedia.org/wiki/Loss_functions_for_classification#Cross_entropy_loss_(Log_Loss)
 */
public class LogLossFunc implements UnaryLossFunc {
	public LogLossFunc() { }

	@Override
	public double loss(double eta, double y) {
		double d = eta * y;
		if (d < -37) {
			return -d;
		} else if (d > 34) {
			return 0;
		}
		return Math.log(1 + Math.exp(-d));
	}

	@Override
	public double derivative(double eta, double y) {
		double d = eta * y;
		if (d < -37) {
			return -y;
		} else {
			return -y / (Math.exp(d) + 1);
		}
	}

	@Override
	public double secondDerivative(double eta, double y) {
		double t = y / (1 + Math.exp(eta * y));
		return t * (y - t);
	}
}
