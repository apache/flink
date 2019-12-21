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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.shaded.asm7.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm7.org.objectweb.asm.ClassWriter;
import org.apache.flink.shaded.asm7.org.objectweb.asm.commons.ClassRemapper;
import org.apache.flink.shaded.asm7.org.objectweb.asm.commons.SimpleRemapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class relocator for testing, this will take a given root class, reload it in a new {@link
 * ClassLoader}, and thereby path inner definer classes that have the {@link RelocateClass}
 * annotation to have a different name.
 */
public final class ClassRelocator {

	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface RelocateClass {
		String value();
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<? extends T> relocate(
		Class<?> originalClass) {

		ClassRegistry remapping = new ClassRegistry(originalClass);
		ClassRenamer classRenamer = new ClassRenamer(remapping);
		Map<String, byte[]> newClassBytes = classRenamer.remap();

		return (Class<? extends T>) patchClass(newClassBytes, remapping);
	}

	private static Class<?> patchClass(Map<String, byte[]> newClasses, ClassRegistry remapping) {
		final ByteClassLoader renamingClassLoader = new ByteClassLoader(remapping.getRoot().getClassLoader(), newClasses);
		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(renamingClassLoader)) {
			return renamingClassLoader.loadClass(remapping.getRootNewName());
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final class ClassRegistry {
		private static final AtomicInteger GENERATED_ID = new AtomicInteger(0);

		private final Class<?> root;
		private final Map<Class<?>, String> targetNames;

		ClassRegistry(Class<?> root) {
			this.root = root;
			this.targetNames = definedClasses(root)
				.filter(type -> type.getAnnotation(RelocateClass.class) != null)
				.collect(Collectors.toMap(
					Function.identity(),
					type -> type.getAnnotation(RelocateClass.class).value()
				));

			// it is also important to overwrite the top class since it is already loaded,
			// and we need to force load it
			targetNames.put(root, root.getName() + String.format("$generated%d$", GENERATED_ID.incrementAndGet()));
		}

		public Class<?> getRoot() {
			return root;
		}

		String getRootNewName() {
			return newNameFor(root);
		}

		String newNameFor(Class<?> oldClass) {
			return targetNames.getOrDefault(oldClass, oldClass.getName());
		}

		Set<Class<?>> getDefinedClassesUnderRoot() {
			return definedClasses(root)
				.collect(Collectors.toSet());
		}

		private Map<String, String> remappingAsPathNames() {
			return targetNames.entrySet()
				.stream()
				.collect(Collectors.toMap(
					e -> pathName(e.getKey()),
					e -> pathName(e.getValue())));
		}

		private static String pathName(String className) {
			return className.replace('.', '/');
		}

		private static String pathName(Class<?> javaClass) {
			return javaClass.getName().replace('.', '/');
		}

		private static Stream<Class<?>> definedClasses(Class<?> klass) {
			Stream<Class<?>> nestedClass = Arrays.stream(klass.getClasses())
				.flatMap(ClassRegistry::definedClasses);

			return Stream.concat(
				Stream.of(klass),
				nestedClass);
		}
	}

	private static final class ClassRenamer {
		private final ClassRegistry renaming;

		ClassRenamer(ClassRegistry renaming) {
			this.renaming = renaming;
		}

		Map<String, byte[]> remap() {
			final Map<String, String> renames = renaming.remappingAsPathNames();

			return renaming.getDefinedClassesUnderRoot()
				.stream()
				.collect(Collectors.toMap(renaming::newNameFor, classToTransform -> {
					ClassReader providerClassReader = classReaderFor(classToTransform);
					ClassWriter transformedProvider = remap(renames, providerClassReader);
					return transformedProvider.toByteArray();
				}));
		}

		private static ClassWriter remap(Map<String, String> reMapping, ClassReader providerClassReader) {
			ClassWriter cw = new ClassWriter(0);
			ClassRemapper remappingClassAdapter = new ClassRemapper(cw, new SimpleRemapper(reMapping));
			providerClassReader.accept(remappingClassAdapter, ClassReader.EXPAND_FRAMES);
			return cw;
		}

		private static ClassReader classReaderFor(Class<?> providerClass) {
			String classAsPath = providerClass.getName().replace('.', '/') + ".class";
			InputStream in = providerClass.getClassLoader().getResourceAsStream(classAsPath);
			try {
				return new ClassReader(in);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final class ByteClassLoader extends URLClassLoader {
		private final Map<String, byte[]> remappedClasses;

		private ByteClassLoader(ClassLoader parent, Map<String, byte[]> remappedClasses) {
			super(new URL[0], parent);
			this.remappedClasses = remappedClasses;
		}

		@Override
		protected Class<?> findClass(String name) throws ClassNotFoundException {
			byte[] bytes = remappedClasses.remove(name);
			if (bytes == null) {
				return super.findClass(name);
			}
			return defineClass(name, bytes, 0, bytes.length);
		}

	}
}
