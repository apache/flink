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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputStreamOperatorBase;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapper;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Describe the info of {@link Input}.
 */
public class InputSpec implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * The input id (start from 1) used for identifying each {@link Input}
	 * in {@link MultipleInputStreamOperatorBase#getInputs()}.
	 */
	private final int multipleInputId;

	/**
	 * The read order for current input in multiple operator.
	 */
	private final int readOrder;

	/**
	 * The output operator corresponding to the {@link Input}.
	 */
	private final TableOperatorWrapper<?> output;

	/**
	 * The input id (start from 1) is used for identifying each input of the output operator.
	 */
	private final int outputOpInputId;

	public InputSpec(
			int multipleInputId,
			int readOrder,
			TableOperatorWrapper<?> output,
			int outputOpInputId) {
		this.multipleInputId = multipleInputId;
		this.readOrder = readOrder;
		this.output = checkNotNull(output);
		this.outputOpInputId = outputOpInputId;
	}

	public int getMultipleInputId() {
		return multipleInputId;
	}

	public int getReadOrder() {
		return readOrder;
	}

	public TableOperatorWrapper<?> getOutput() {
		return output;
	}

	public int getOutputOpInputId() {
		return outputOpInputId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		InputSpec inputSpec = (InputSpec) o;
		return multipleInputId == inputSpec.multipleInputId &&
				readOrder == inputSpec.readOrder &&
				outputOpInputId == inputSpec.outputOpInputId &&
				output.equals(inputSpec.output);
	}

	@Override
	public int hashCode() {
		return Objects.hash(multipleInputId, readOrder, output, outputOpInputId);
	}

	@Override
	public String toString() {
		return "InputSpec{" +
				"multipleInputId=" + multipleInputId +
				", readOrder=" + readOrder +
				", output=" + output +
				", outputOpInputId=" + outputOpInputId +
				'}';
	}
}
