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
package eu.stratosphere.api.java.operators.translation;

import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.FunctionAnnotation;
import eu.stratosphere.api.java.functions.SemanticPropUtil;
import eu.stratosphere.api.java.typeutils.TypeInformation;

import java.lang.annotation.Annotation;
import java.util.Set;

public class PlanCogroupOperator<IN1, IN2, OUT>
	extends CoGroupOperatorBase<GenericCoGrouper<IN1, IN2, OUT>>
	implements BinaryJavaPlanNode<IN1, IN2, OUT> {

	private final TypeInformation<IN1> inType1;
	private final TypeInformation<IN2> inType2;
	private final TypeInformation<OUT> outType;

	public PlanCogroupOperator(
			CoGroupFunction<IN1, IN2, OUT> udf,
			int[] keyPositions1, int[] keyPositions2, String name, TypeInformation<IN1> inType1, TypeInformation<IN2> inType2, TypeInformation<OUT> outType) {
		super(udf, keyPositions1, keyPositions2, name);

		this.inType1 = inType1;
		this.inType2 = inType2;
		this.outType = outType;

		Set<Annotation> annotations = FunctionAnnotation.readDualConstantAnnotations(this.getUserCodeWrapper());
		DualInputSemanticProperties dsp = SemanticPropUtil.getSemanticPropsDual(annotations, this.getInputType1(), this.getInputType2(), this.getReturnType());
		this.setSemanticProperties(dsp);
	}

	@Override
	public TypeInformation<OUT> getReturnType() {
		return this.outType;
	}

	@Override
	public TypeInformation<IN1> getInputType1() {
		return this.inType1;
	}

	@Override
	public TypeInformation<IN2> getInputType2() {
		return this.inType2;
	}
}
