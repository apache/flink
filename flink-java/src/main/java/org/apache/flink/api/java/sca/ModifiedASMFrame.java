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

package org.apache.flink.api.java.sca;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.AbstractInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.AnalyzerException;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Frame;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Interpreter;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Value;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Modified version of ASMs Frame. It allows to perform different merge
 * priorities and passes frame information to the Interpreter.
 */
@Internal
public class ModifiedASMFrame extends Frame {

	public boolean mergePriority;

	public ModifiedASMFrame(int nLocals, int nStack) {
		super(nLocals, nStack);
	}

	public ModifiedASMFrame(Frame src) {
		super(src);
	}

	@Override
	public Frame init(Frame src) {
		mergePriority = ((ModifiedASMFrame) src).mergePriority;
		return super.init(src);
	}

	@Override
	public void execute(AbstractInsnNode insn, Interpreter interpreter)
			throws AnalyzerException {
		NestedMethodAnalyzer nma = ((NestedMethodAnalyzer) interpreter);
		nma.currentFrame = (ModifiedASMFrame) this;
		super.execute(insn, interpreter);
	}

	@Override
	public boolean merge(Frame frame, Interpreter interpreter) throws AnalyzerException {
		if (((ModifiedASMFrame) frame).mergePriority) {
			((NestedMethodAnalyzer) interpreter).rightMergePriority = true;
		}
		final boolean result = super.merge(frame, interpreter);
		((NestedMethodAnalyzer) interpreter).rightMergePriority = false;
		((ModifiedASMFrame) frame).mergePriority = false;
		return result;
	}

	@Override
	public String toString() {
		// FOR DEBUGGING
		try {
			Class<?> frame = Frame.class;
			Field valuesField = frame.getDeclaredField("values");
			valuesField.setAccessible(true);
			Value[] newValues = (Value[]) valuesField.get(this);
			return Arrays.toString(newValues);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
