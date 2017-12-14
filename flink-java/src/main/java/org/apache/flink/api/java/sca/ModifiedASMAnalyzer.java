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
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.InsnList;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.JumpInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Analyzer;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Frame;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Interpreter;

import java.lang.reflect.Field;

/**
 * Modified version of ASMs Analyzer. It defines a custom ASM Frame
 * and allows jump modification which is necessary for UDFs with
 * one iterable input e.g. GroupReduce.
 * (see also UdfAnalyzer's field "iteratorTrueAssumptionApplied")
 */
@Internal
public class ModifiedASMAnalyzer extends Analyzer {

	private NestedMethodAnalyzer interpreter;

	public ModifiedASMAnalyzer(Interpreter interpreter) {
		super(interpreter);
		this.interpreter = (NestedMethodAnalyzer) interpreter;
	}

	protected Frame newFrame(int nLocals, int nStack) {
		return new ModifiedASMFrame(nLocals, nStack);
	}

	protected Frame newFrame(Frame src) {
		return new ModifiedASMFrame(src);
	}

	// type of jump modification
	private int jumpModification = NO_MOD;
	private static final int NO_MOD = -1;
	private static final int IFEQ_MOD = 0;
	private static final int IFNE_MOD = 1;
	private int eventInsn;

	// current state of modification
	private int jumpModificationState = DO_NOTHING;
	private static final int DO_NOTHING = -1;
	private static final int PRE_STATE = 0;
	private static final int MOD_STATE = 1;
	private static final int WAIT_FOR_INSN_STATE = 2;

	public void requestIFEQLoopModification() {
		if (jumpModificationState != DO_NOTHING) {
			throw new CodeAnalyzerException("Unable to do jump modifications (unsupported nested jumping).");
		}
		jumpModification = IFEQ_MOD;
		jumpModificationState = PRE_STATE;
	}

	public void requestIFNELoopModification() {
		if (jumpModificationState != DO_NOTHING) {
			throw new CodeAnalyzerException("Unable to do jump modifications (unsupported nested jumping).");
		}
		jumpModification = IFNE_MOD;
		jumpModificationState = PRE_STATE;
	}

	@Override
	protected void newControlFlowEdge(int insn, int successor) {
		try {
			if (jumpModificationState == PRE_STATE) {
				jumpModificationState = MOD_STATE;
			}
			else if (jumpModificationState == MOD_STATE) {
				// this modification swaps the top 2 values on the queue stack
				// it ensures that the "TRUE" path will be executed first, which is important
				// for a later merge
				if (jumpModification == IFEQ_MOD) {
					final int top = accessField(Analyzer.class, "top").getInt(this);
					final int[] queue = (int[]) accessField(Analyzer.class, "queue").get(this);

					final int tmp = queue[top - 2];
					queue[top - 2] = queue[top - 1];
					queue[top - 1] = tmp;

					eventInsn = queue[top - 2] - 1;
					final InsnList insns = (InsnList) accessField(Analyzer.class, "insns").get(this);
					// check if instruction is a goto instruction
					// if yes this is loop structure
					if (insns.get(eventInsn) instanceof JumpInsnNode) {
						jumpModificationState = WAIT_FOR_INSN_STATE;
					}
					// no loop -> end of modification
					else {
						jumpModificationState = DO_NOTHING;
					}
				}
				// this modification changes the merge priority of certain frames (the expression part of the IF)
				else if (jumpModification == IFNE_MOD) {
					final Frame[] frames = (Frame[]) accessField(Analyzer.class, "frames").get(this);
					final Field indexField = accessField(AbstractInsnNode.class, "index");

					final InsnList insns = (InsnList) accessField(Analyzer.class, "insns").get(this);
					final AbstractInsnNode gotoInsnn = insns.get(successor - 1);
					// check for a loop
					if (gotoInsnn instanceof JumpInsnNode) {
						jumpModificationState = WAIT_FOR_INSN_STATE;
						// sets a merge priority for all instructions (the expression of the IF)
						// from the label the goto instruction points to until the evaluation with IFEQ
						final int idx = indexField.getInt(accessField(JumpInsnNode.class, "label").get(gotoInsnn));

						for (int i = idx; i <= insn; i++) {
							((ModifiedASMFrame) frames[i]).mergePriority = true;
						}
						eventInsn = idx - 2;
					}
					else {
						jumpModificationState = DO_NOTHING;
					}
				}
			}
			// wait for the goto instruction
			else if (jumpModificationState == WAIT_FOR_INSN_STATE && insn == eventInsn) {
				jumpModificationState = DO_NOTHING;
				final Frame[] frames = (Frame[]) accessField(Analyzer.class, "frames").get(this);
				// merge the goto instruction frame with the frame that follows
				// this ensures that local variables are kept
				if (jumpModification == IFEQ_MOD) {
					interpreter.rightMergePriority = true;
					final Field top = accessField(Frame.class, "top");
					top.setInt(frames[eventInsn], top.getInt(frames[eventInsn + 1]));
					frames[eventInsn + 1].merge(frames[eventInsn], interpreter);
				}
				// finally set a merge priority for the last instruction of the loop (before the IF expression)
				else if (jumpModification == IFNE_MOD) {
					((ModifiedASMFrame) frames[eventInsn + 1]).mergePriority = true;
				}
			}
		}
		catch (Exception e) {
			throw new CodeAnalyzerException("Unable to do jump modifications.", e);
		}
	}

	private Field accessField(Class<?> clazz, String name) {
		for (Field f : clazz.getDeclaredFields()) {
			if (f.getName().equals(name)) {
				f.setAccessible(true);
				return f;
			}
		}
		return null;
	}
}
