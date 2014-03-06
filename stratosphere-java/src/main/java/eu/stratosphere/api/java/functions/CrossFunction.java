/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCrosser;
import eu.stratosphere.util.Collector;


public abstract class CrossFunction<IN1, IN2, OUT> extends AbstractFunction implements GenericCrosser<IN1, IN2, OUT>{
	
	private static final long serialVersionUID = 1L;
	

	public abstract OUT cross(IN1 first, IN2 second) throws Exception;
	
	
	
	@Override
	public final void cross(IN1 record1, IN2 record2, Collector<OUT> out) throws Exception {
		out.collect(cross(record1, record2));
	}
}
