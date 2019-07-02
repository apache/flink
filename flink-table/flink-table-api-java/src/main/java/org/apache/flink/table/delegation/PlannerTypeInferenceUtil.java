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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Temporary utility for validation and output type inference until all {@code PlannerExpression} are
 * upgraded to work with {@link TypeInferenceUtil}.
 */
@Internal
public interface PlannerTypeInferenceUtil {

	static PlannerTypeInferenceUtil create() {
		return SingletonPlannerTypeInferenceUtil.getPlannerTypeInferenceUtil();
	}

	/**
	 * Same behavior as {@link TypeInferenceUtil#runTypeInference(TypeInference, CallContext)}.
	 */
	TypeInferenceUtil.Result runTypeInference(
		UnresolvedCallExpression unresolvedCall,
		List<ResolvedExpression> resolvedArgs);

	/**
	 * A singleton pattern utility for avoiding creating many {@link PlannerTypeInferenceUtil}.
	 */
	class SingletonPlannerTypeInferenceUtil {

		private static PlannerTypeInferenceUtil plannerTypeInferenceUtil;

		public static PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
			if (plannerTypeInferenceUtil == null) {
				try {
					final Class<?> clazz =
						Class.forName("org.apache.flink.table.expressions.PlannerTypeInferenceUtilImpl");
					final Constructor<?> con = clazz.getConstructor();
					plannerTypeInferenceUtil = (PlannerTypeInferenceUtil) con.newInstance();
				} catch (Throwable t) {
					throw new TableException("Instantiation of PlannerTypeInferenceUtil failed.", t);
				}
			}
			return plannerTypeInferenceUtil;
		}

		private SingletonPlannerTypeInferenceUtil() {
			// no instantiation
		}
	}
}
