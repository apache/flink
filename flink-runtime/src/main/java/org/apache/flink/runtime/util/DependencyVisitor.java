/**
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

package org.apache.flink.runtime.util;

import org.apache.flink.shaded.asm5.org.objectweb.asm.AnnotationVisitor;
import org.apache.flink.shaded.asm5.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm5.org.objectweb.asm.Opcodes;
import org.apache.flink.shaded.asm5.org.objectweb.asm.FieldVisitor;
import org.apache.flink.shaded.asm5.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm5.org.objectweb.asm.Type;
import org.apache.flink.shaded.asm5.org.objectweb.asm.TypePath;
import org.apache.flink.shaded.asm5.org.objectweb.asm.Label;
import org.apache.flink.shaded.asm5.org.objectweb.asm.signature.SignatureReader;
import org.apache.flink.shaded.asm5.org.objectweb.asm.signature.SignatureVisitor;

import java.util.HashSet;
import java.util.Set;


/**
 * This class tracks class dependency with ASM visitors.
 * The tutorial could be found here http://asm.ow2.org/doc/tutorial-asm-2.0.html
 *
 */
public class DependencyVisitor extends ClassVisitor {

	private Set<String> packages = new HashSet<String>();

	private Set<String> nameSpace = new HashSet<String>();

	public Set<String> getPackages() {
		return packages;
	}


	public DependencyVisitor(int api) {
		super(api);
	}

	@Override
	public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
		if (signature == null) {
			addInternalName(superName);
			addInternalNames(interfaces);
		} else {
			addSignature(signature);
		}
	}

	@Override
	public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
		addDesc(desc);
		return new AnnotationVisitorImpl(Opcodes.ASM5);
	}

	@Override
	public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
		if (signature == null) {
			addDesc(desc);
		} else {
			addTypeSignature(signature);
		}
		if (value instanceof Type) {
			addType((Type) value);
		}
		return new FieldVisitorImpl(Opcodes.ASM5);
	}

	@Override
	public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
		if (signature == null) {
			addMethodDesc(desc);
		} else {
			addSignature(signature);
		}
		addInternalNames(exceptions);
		return new MethodVisitorImpl(Opcodes.ASM5);
	}

	// ---------------------------------------------------------------------------------------------

	public void addNameSpace(Set<String> names) {
		for (String name : names) {
			this.nameSpace.add(name.replace('.', '/'));
		}
	}

	private boolean checkUserDefine(String name) {
		String[] ns = {};
		ns = nameSpace.toArray(ns);

		for (String s : ns) {
			if (name.startsWith(s)) {
				return true;
			}
		}
		return false;
	}

	private void addName(final String name) {
		if (checkUserDefine(name)) {
			packages.add(name);
		}
	}

	private void addInternalName(String name) {
		addType(Type.getObjectType(name));
	}

	private void addInternalNames(String[] names) {
		for (int i = 0; names != null && i < names.length; i++) {
			addInternalName(names[i]);
		}
	}

	private void addDesc(String desc) {
		addType(Type.getType(desc));
	}

	private void addMethodDesc(String desc) {
		addType(Type.getReturnType(desc));
		Type[] types = Type.getArgumentTypes(desc);
		for (int i = 0; i < types.length; i++) {
			addType(types[i]);
		}
	}

	private void addType(Type t) {
		switch (t.getSort()) {
			case Type.ARRAY:
				addType(t.getElementType());
				break;
			case Type.OBJECT:
				addName(t.getInternalName());
				break;
		}
	}

	private void addSignature(String signature) {
		if (signature != null) {
			new SignatureReader(signature).accept(new SignatureVisitorImpl(Opcodes.ASM5));
		}
	}

	private void addTypeSignature(String signature) {
		if (signature != null) {
			new SignatureReader(signature).acceptType(new SignatureVisitorImpl(Opcodes.ASM5));
		}
	}

	public class MethodVisitorImpl extends MethodVisitor {

		public MethodVisitorImpl(int api) {
			super(api);
		}

		@Override
		public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
			addDesc(desc);
			return new AnnotationVisitorImpl(Opcodes.ASM5);
		}

		@Override
		public void visitTypeInsn(int opcode, String type) {
			addType(Type.getObjectType(type));
		}

		@Override
		public void visitFieldInsn(int opcode, String owner, String name, String desc) {
			addInternalName(owner);
			addDesc(desc);
		}

		@Override
		public void visitMethodInsn(int opcode, String owner, String name, String desc) {
			addInternalName(owner);
			addMethodDesc(desc);
		}

		@Override
		public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
			addInternalName(owner);
			addMethodDesc(desc);
		}

		@Override
		public void visitLdcInsn(Object cst) {
			if (cst instanceof Type) {
				addType((Type) cst);
			}
		}

		@Override
		public void visitMultiANewArrayInsn(String desc, int dims) {
			addDesc(desc);
		}

		@Override
		public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
			if (signature == null) {
				addDesc(desc);
			} else {
				addTypeSignature(signature);
			}
		}

		@Override
		public AnnotationVisitor visitAnnotationDefault() {
			return new AnnotationVisitorImpl(Opcodes.ASM5);
		}

		@Override
		public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
			addDesc(desc);
			return new AnnotationVisitorImpl(Opcodes.ASM5);
		}

		@Override
		public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
			if (type != null) {
				addInternalName(type);
			}
		}
	}

	public class AnnotationVisitorImpl extends AnnotationVisitor {
		public AnnotationVisitorImpl(int api) {
			super(api);
		}

		@Override
		public void visit(String name, Object value) {
			if (value instanceof Type) {
				addType((Type) value);
			}
		}

		@Override
		public void visitEnum(String name, String desc, String value) {
			addDesc(desc);
		}

		@Override
		public AnnotationVisitor visitAnnotation(String name, String desc) {
			addDesc(desc);
			return this;
		}

		@Override
		public AnnotationVisitor visitArray(String name) {
			return this;
		}
	}

	public class FieldVisitorImpl extends FieldVisitor {

		public FieldVisitorImpl(int api) {
			super(api);
		}

		@Override
		public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
			addDesc(desc);
			return new AnnotationVisitorImpl(Opcodes.ASM5);
		}
	}

	public class SignatureVisitorImpl extends SignatureVisitor {

		private String signatureClassName;

		private boolean newParameter = false;

		public SignatureVisitorImpl(int api) {
			super(api);
		}

		@Override
		public SignatureVisitor visitParameterType() {
			this.newParameter = true;
			return this;
		}

		@Override
		public SignatureVisitor visitReturnType() {
			this.newParameter = true;
			return this;
		}

		@Override
		public void visitClassType(String name) {
			if (signatureClassName == null || this.newParameter) {
				signatureClassName = name;
				newParameter = false;
			}
			addInternalName(name);
		}

		@Override
		public void visitInnerClassType(String name) {
			signatureClassName = signatureClassName + "$" + name;
			addInternalName(signatureClassName);
		}
	}
}
