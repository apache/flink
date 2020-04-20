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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

/**
 * Java GC Cleaner wrapper.
 *
 * <p>A clean operation can be wrapped with the Java GC Cleaner
 * which will schedule this operation before GC is run for the given owner object (not reachable in user code).
 * but only if the cleaner has not been already run explicitly by user before.
 * If the cleaner is run after GC it will not run clean operation again.
 * This way we guarantee that the clean operation will always run at some point but only once.
 *
 * <p>The wrapper looks up the underlying Java GC Cleaner class in different packages
 */
public enum JavaGcCleanerWrapper {
	;

	private static final Logger LOG = LoggerFactory.getLogger(JavaGcCleanerWrapper.class);

	private static final Collection<CleanerProvider> CLEANER_PROVIDERS =
		Arrays.asList(LegacyCleanerProvider.INSTANCE, Java9CleanerProvider.INSTANCE);
	private static final CleanerFactory CLEANER_FACTORY = findGcCleaner();

	private static CleanerFactory findGcCleaner() {
		CleanerFactory foundCleanerFactory = null;
		Throwable t = null;
		for (CleanerProvider cleanerProvider : CLEANER_PROVIDERS) {
			//noinspection OverlyBroadCatchBlock
			try {
				foundCleanerFactory = cleanerProvider.createCleanerFactory();
				break;
			} catch (Throwable e) {
				t = ExceptionUtils.firstOrSuppressed(e, t);
			}
		}

		if (foundCleanerFactory == null) {
			String errorMessage = String.format("Failed to find GC Cleaner among available providers: %s", CLEANER_PROVIDERS);
			throw new Error(errorMessage, t);
		}
		return foundCleanerFactory;
	}

	public static Runnable create(Object owner, Runnable cleanOperation) {
		return CLEANER_FACTORY.create(owner, cleanOperation);
	}

	@FunctionalInterface
	private interface CleanerProvider {
		CleanerFactory createCleanerFactory() throws ClassNotFoundException;
	}

	@FunctionalInterface
	private interface CleanerFactory {
		Runnable create(Object owner, Runnable cleanOperation);
	}

	private enum LegacyCleanerProvider implements CleanerProvider {
		INSTANCE;

		private static final String LEGACY_CLEANER_CLASS_NAME = "sun.misc.Cleaner";

		@Override
		public CleanerFactory createCleanerFactory() {
			Class<?> cleanerClass = findCleanerClass();
			Method cleanerCreateMethod = getCleanerCreateMethod(cleanerClass);
			Method cleanerCleanMethod = getCleanerCleanMethod(cleanerClass);
			return new LegacyCleanerFactory(cleanerCreateMethod, cleanerCleanMethod);
		}

		private static Class<?> findCleanerClass() {
			try {
				return Class.forName(LEGACY_CLEANER_CLASS_NAME);
			} catch (ClassNotFoundException e) {
				throw new FlinkRuntimeException("Failed to find Java legacy Cleaner class", e);
			}
		}

		private static Method getCleanerCreateMethod(Class<?> cleanerClass) {
			try {
				return cleanerClass.getMethod("create", Object.class, Runnable.class);
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException("Failed to find Java legacy Cleaner#create method", e);
			}
		}

		private static Method getCleanerCleanMethod(Class<?> cleanerClass) {
			try {
				return cleanerClass.getMethod("clean");
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException("Failed to find Java legacy Cleaner#clean method", e);
			}
		}

		@Override
		public String toString() {
			return "Legacy cleaner provider before Java 9 using " + LEGACY_CLEANER_CLASS_NAME;
		}
	}

	private static final class LegacyCleanerFactory implements CleanerFactory {
		private final Method cleanerCreateMethod;
		private final Method cleanerCleanMethod;

		private LegacyCleanerFactory(Method cleanerCreateMethod, Method cleanerCleanMethod) {
			this.cleanerCreateMethod = cleanerCreateMethod;
			this.cleanerCleanMethod = cleanerCleanMethod;
		}

		@Override
		public Runnable create(Object owner, Runnable cleanupOperation) {
			Object cleaner;
			try {
				cleaner = cleanerCreateMethod.invoke(null, owner, cleanupOperation);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new Error("Failed to create a Java legacy Cleaner", e);
			}
			String ownerString = owner.toString(); // lambda should not capture the owner object
			return () -> {
				try {
					cleanerCleanMethod.invoke(cleaner);
				} catch (IllegalAccessException | InvocationTargetException e) {
					String message = String.format("FATAL UNEXPECTED - Failed to invoke a Java legacy Cleaner for %s", ownerString);
					LOG.error(message, e);
					throw new Error(message, e);
				}
			};
		}
	}

	/** New cleaner provider for Java 9+. */
	private enum Java9CleanerProvider implements CleanerProvider {
		INSTANCE;

		private static final String JAVA9_CLEANER_CLASS_NAME = "java.lang.ref.Cleaner";

		@Override
		public CleanerFactory createCleanerFactory() {
			Class<?> cleanerClass = findCleanerClass();
			Method cleanerCreateMethod = getCleanerCreateMethod(cleanerClass);
			Object cleaner = createCleaner(cleanerCreateMethod);
			Method cleanerRegisterMethod = getCleanerRegisterMethod(cleanerClass);
			Class<?> cleanableClass = findCleanableClass();
			Method cleanMethod = getCleanMethod(cleanableClass);
			return new Java9CleanerFactory(cleaner, cleanerRegisterMethod, cleanMethod);
		}

		private static Class<?> findCleanerClass() {
			try {
				return Class.forName(JAVA9_CLEANER_CLASS_NAME);
			} catch (ClassNotFoundException e) {
				throw new FlinkRuntimeException("Failed to find Java 9 Cleaner class", e);
			}
		}

		private static Method getCleanerCreateMethod(Class<?> cleanerClass) {
			try {
				return cleanerClass.getMethod("create");
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException("Failed to find Java 9 Cleaner#create method", e);
			}
		}

		private static Object createCleaner(Method cleanerCreateMethod) {
			try {
				return cleanerCreateMethod.invoke(null);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new FlinkRuntimeException("Failed to create a Java 9 Cleaner", e);
			}
		}

		private static Method getCleanerRegisterMethod(Class<?> cleanerClass) {
			try {
				return cleanerClass.getMethod("register", Object.class, Runnable.class);
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException("Failed to find Java 9 Cleaner#create method", e);
			}
		}

		private static Class<?> findCleanableClass() {
			try {
				return Class.forName("java.lang.ref.Cleaner$Cleanable");
			} catch (ClassNotFoundException e) {
				throw new FlinkRuntimeException("Failed to find Java 9 Cleaner#Cleanable class", e);
			}
		}

		private static Method getCleanMethod(Class<?> cleanableClass) {
			try {
				return cleanableClass.getMethod("clean");
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException("Failed to find Java 9 Cleaner$Cleanable#clean method", e);
			}
		}

		@Override
		public String toString() {
			return "New cleaner provider for Java 9+" + JAVA9_CLEANER_CLASS_NAME;
		}
	}

	private static final class Java9CleanerFactory implements CleanerFactory {
		private final Object cleaner;
		private final Method cleanerRegisterMethod;
		private final Method cleanMethod;

		private Java9CleanerFactory(Object cleaner, Method cleanerRegisterMethod, Method cleanMethod) {
			this.cleaner = cleaner;
			this.cleanerRegisterMethod = cleanerRegisterMethod;
			this.cleanMethod = cleanMethod;
		}

		@Override
		public Runnable create(Object owner, Runnable cleanupOperation) {
			Object cleanable;
			try {
				cleanable = cleanerRegisterMethod.invoke(cleaner, owner, cleanupOperation);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new Error("Failed to create a Java 9 Cleaner", e);
			}
			String ownerString = owner.toString(); // lambda should not capture the owner object
			return () -> {
				try {
					cleanMethod.invoke(cleanable);
				} catch (IllegalAccessException | InvocationTargetException e) {
					String message = String.format("FATAL UNEXPECTED - Failed to invoke a Java 9 Cleaner$Cleanable for %s", ownerString);
					LOG.error(message, e);
					throw new Error(message, e);
				}
			};
		}
	}
}
