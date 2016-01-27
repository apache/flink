/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java;


import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.util.InstantiationUtil;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

public class ClosureCleaner {
	private static Logger LOG = LoggerFactory.getLogger(ClosureCleaner.class);

	private static ClassReader getClassReader(Class<?> cls) {
		String className = cls.getName().replaceFirst("^.*\\.", "") + ".class";
		try {
			return new ClassReader(cls.getResourceAsStream(className));
		} catch (IOException e) {
			throw new RuntimeException("Could not create ClassReader: " + e);
		}
	}

	public static void clean(Object func, boolean checkSerializable) {
		Class<?> cls = func.getClass();

		// First find the field name of the "this$0" field, this can
		// be "field$x" depending on the nesting
		for (Field f: cls.getDeclaredFields()) {
			if (f.getName().startsWith("this$")) {
				// found our field:
				cleanThis0(func, cls, f.getName());
			}
		}

		if (checkSerializable) {
			ensureSerializable(func);
		}
	}

	private static void cleanThis0(Object func, Class<?> cls, String this0Name) {

		This0AccessFinder this0Finder = new This0AccessFinder(this0Name);

		getClassReader(cls).accept(this0Finder, 0);


		if (LOG.isDebugEnabled()) {
			LOG.debug(this0Name + " is accessed: " + this0Finder.isThis0Accessed());
		}

		if (!this0Finder.isThis0Accessed()) {
			Field this0;
			try {
				this0 = func.getClass().getDeclaredField(this0Name);
			} catch (NoSuchFieldException e) {
				// has no this$0, just return
				throw new RuntimeException("Could not set " + this0Name + ": " + e);
			}
			this0.setAccessible(true);
			try {
				this0.set(func, null);
			} catch (IllegalAccessException e) {
				// should not happen, since we use setAccessible
				throw new RuntimeException("Could not set " + this0Name + ": " + e);
			}
		}
	}


	public static void ensureSerializable(Object obj) {
		try {
			InstantiationUtil.serializeObject(obj);
		} catch (Exception e) {
			throw new InvalidProgramException("Object " + obj + " not serializable", e);
		}
	}

}

class This0AccessFinder extends ClassVisitor {
	private boolean isThis0Accessed = false;
	private String this0Name;

	public This0AccessFinder(String this0Name) {
		super(Opcodes.ASM5);
		this.this0Name = this0Name;
	}

	public boolean isThis0Accessed() {
		return isThis0Accessed;
	}

	@Override
	public MethodVisitor visitMethod(int access, String name, String desc, String sig, String[] exceptions) {
		return new MethodVisitor(Opcodes.ASM5) {

			@Override
			public void visitFieldInsn(int op, String owner, String name, String desc) {
				if (op == Opcodes.GETFIELD && name.equals(this0Name)) {
					isThis0Accessed = true;
				}
			}
		};
	}
}
