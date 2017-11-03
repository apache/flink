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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.asm5.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm5.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm5.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm5.org.objectweb.asm.Opcodes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * The closure cleaner is a utility that tries to truncate the closure (enclosing instance)
 * of non-static inner classes (created for inline transformation functions). That makes non-static
 * inner classes in many cases serializable, where Java's default behavior renders them non-serializable
 * without good reason.
 */
@Internal
public class ClosureCleaner {

	private static final Logger LOG = LoggerFactory.getLogger(ClosureCleaner.class);

	/**
	 * Tries to clean the closure of the given object, if the object is a non-static inner
	 * class.
	 *
	 * @param func The object whose closure should be cleaned.
	 * @param checkSerializable Flag to indicate whether serializability should be checked after
	 *                          the closure cleaning attempt.
	 *
	 * @throws InvalidProgramException Thrown, if 'checkSerializable' is true, and the object was
	 *                                 not serializable after the closure cleaning.
	 *
	 * @throws RuntimeException A RuntimeException may be thrown, if the code of the class could not
	 *                          be loaded, in order to process during teh closure cleaning.
	 */
	public static void clean(Object func, boolean checkSerializable) {
		if (func == null) {
			return;
		}

		final Class<?> cls = func.getClass();

		// First find the field name of the "this$0" field, this can
		// be "this$x" depending on the nesting
		boolean closureAccessed = false;

		for (Field f: cls.getDeclaredFields()) {
			if (f.getName().startsWith("this$")) {
				// found a closure referencing field - now try to clean
				closureAccessed |= cleanThis0(func, cls, f.getName());
			}
		}

		if (checkSerializable) {
			try {
				InstantiationUtil.serializeObject(func);
			}
			catch (Exception e) {
				String functionType = getSuperClassOrInterfaceName(func.getClass());

				String msg = functionType == null ?
						(func + " is not serializable.") :
						("The implementation of the " + functionType + " is not serializable.");

				if (closureAccessed) {
					msg += " The implementation accesses fields of its enclosing class, which is " +
							"a common reason for non-serializability. " +
							"A common solution is to make the function a proper (non-inner) class, or " +
							"a static inner class.";
				} else {
					msg += " The object probably contains or references non serializable fields.";
				}

				throw new InvalidProgramException(msg, e);
			}
		}
	}

	public static void ensureSerializable(Object obj) {
		try {
			InstantiationUtil.serializeObject(obj);
		} catch (Exception e) {
			throw new InvalidProgramException("Object " + obj + " is not serializable", e);
		}
	}

	private static boolean cleanThis0(Object func, Class<?> cls, String this0Name) {

		This0AccessFinder this0Finder = new This0AccessFinder(this0Name);
		getClassReader(cls).accept(this0Finder, 0);

		final boolean accessesClosure = this0Finder.isThis0Accessed();

		if (LOG.isDebugEnabled()) {
			LOG.debug(this0Name + " is accessed: " + accessesClosure);
		}

		if (!accessesClosure) {
			Field this0;
			try {
				this0 = func.getClass().getDeclaredField(this0Name);
			} catch (NoSuchFieldException e) {
				// has no this$0, just return
				throw new RuntimeException("Could not set " + this0Name + ": " + e);
			}

			try {
				this0.setAccessible(true);
				this0.set(func, null);
			}
			catch (Exception e) {
				// should not happen, since we use setAccessible
				throw new RuntimeException("Could not set " + this0Name + " to null. " + e.getMessage(), e);
			}
		}

		return accessesClosure;
	}

	private static ClassReader getClassReader(Class<?> cls) {
		String className = cls.getName().replaceFirst("^.*\\.", "") + ".class";
		try {
			return new ClassReader(cls.getResourceAsStream(className));
		} catch (IOException e) {
			throw new RuntimeException("Could not create ClassReader: " + e.getMessage(), e);
		}
	}

	private static String getSuperClassOrInterfaceName(Class<?> cls) {
		Class<?> superclass = cls.getSuperclass();
		if (superclass.getName().startsWith("org.apache.flink")) {
			return superclass.getSimpleName();
		} else {
			for (Class<?> inFace : cls.getInterfaces()) {
				if (inFace.getName().startsWith("org.apache.flink")) {
					return inFace.getSimpleName();
				}
			}
			return null;
		}
	}
}

/**
 * This visitor walks methods and finds accesses to the field with the reference to
 * the enclosing class.
 */
class This0AccessFinder extends ClassVisitor {

	private final String this0Name;
	private boolean isThis0Accessed;

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
