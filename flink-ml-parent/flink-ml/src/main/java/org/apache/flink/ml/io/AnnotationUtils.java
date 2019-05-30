/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io;

import org.apache.flink.ml.params.Params;

import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Annotation parser.
 *
 * <p>Use reflections to get the annotation of class.
 */
public class AnnotationUtils {
	/**
	 * Get all class which is indicated by cls.
	 *
	 * @param prefix the prefix of package which is scanned by reflections.
	 * @param cls the annotation class which is searched.
	 * @return the set of classes which have found.
	 */
	public static Set <Class <?>> allAnntated(String prefix, Class cls) {
		Reflections reflections = new Reflections(prefix);
		return reflections.getTypesAnnotatedWith(cls);
	}

	/**
	 * Base class of annotation parser.
	 */
	public static class Annotations {
		Set <Class <?>> clses;

		public Annotations(String prefix, Class cls) {
			this.clses = allAnntated(prefix, cls);
		}

		public Set <Class <?>> getClses() {
			return clses;
		}
	}

	/**
	 * IONameAnnotation parser.
	 */
	public static class NameAnnotations extends Annotations {
		public NameAnnotations() {
			super("org.apache.flink.ml", IONameAnnotation.class);
		}
	}

	/**
	 * IOTypeAnnotation parser.
	 */
	public static class TypeAnnotations extends Annotations {
		public TypeAnnotations() {
			super("org.apache.flink.ml", IOTypeAnnotation.class);
		}
	}

	/**
	 * Singleton of IONameAnnotation parser.
	 */
	public static class NameAnnotationsSingleton {
		private static class InstanceHolder {
			private static final NameAnnotations INSTANCE = new NameAnnotations();
		}

		private NameAnnotationsSingleton() {}

		public static final NameAnnotations getInstance() {
			return InstanceHolder.INSTANCE;
		}
	}

	/**
	 * Singleton of IOTypeAnnotation parser.
	 */
	public static class TypeAnnotationsSingleton {
		private static class InstanceHolder {
			private static final TypeAnnotations INSTANCE = new TypeAnnotations();
		}

		private TypeAnnotationsSingleton() {}

		public static final TypeAnnotations getInstance() {
			return InstanceHolder.INSTANCE;
		}
	}

	public static String annotationName(Class <?> cls) {
		IONameAnnotation notation = cls.getAnnotation(IONameAnnotation.class);
		if (notation == null) {
			return null;
		}

		return notation.name();
	}

	public static boolean annotationStart(Class <?> cls) {
		IONameAnnotation annotation = cls.getAnnotation(IONameAnnotation.class);
		if (annotation == null) {
			return false;
		}

		return annotation.start();
	}

	public static String annotationAlias(Class <?> cls) {
		DBAnnotation annotation = cls.getAnnotation(DBAnnotation.class);

		if (annotation == null) {
			return null;
		}

		return annotation.tableNameAlias();
	}

	public static IOType annotationType(Class <?> cls) {
		IOTypeAnnotation annotation = cls.getAnnotation(IOTypeAnnotation.class);

		if (annotation == null) {
			return null;
		}

		return annotation.type();
	}

	public static Set <Class <?>> allName() {
		return NameAnnotationsSingleton.getInstance().getClses();
	}

	public static Set <Class <?>> allType() {
		return TypeAnnotationsSingleton.getInstance().getClses();
	}

	public static List <Class <?>> pickName(Set <Class <?>> clses, String name) {
		List <Class <?>> ret = new ArrayList <>();
		for (Class <?> cls : clses) {
			if (annotationName(cls).equals(name)) {
				ret.add(cls);
			}
		}

		return ret;
	}

	public static List <Class <?>> pickType(Set <Class <?>> clses, IOType type) {
		List <Class <?>> ret = new ArrayList <>();
		for (Class <?> cls : clses) {
			if (annotationType(cls).equals(type)) {
				ret.add(cls);
			}
		}

		return ret;
	}

	public static boolean isDB(String name) {
		List <Class <?>> picked = pickName(allName(), name);

		if (picked.size() != 1) {
			return false;
		}

		for (Class <?> cls : picked) {
			String tableNameAlias = annotationAlias(cls);

			if (tableNameAlias != null) {
				return true;
			}
		}

		return false;
	}

	public static Class <?> dbCLS(String name) {
		Set <Class <?>> all = allName();

		List <Class <?>> picked = pickName(all, name);

		if (picked.size() > 1) {
			throw new RuntimeException("Pick db error. cls: " + picked);
		}

		if (picked.isEmpty()) {
			return null;
		}

		return picked.get(0);
	}

	public static Class <?> opCLS(String name, IOType type) {
		Set <Class <?>> allName = allName();
		Set <Class <?>> allType = allType();

		List <Class <?>> pickedNames = pickName(allName, name);
		List <Class <?>> pickedTypes = pickType(allType, type);

		Class <?> ret = null;
		for (Class <?> pickedName : pickedNames) {
			for (Class <?> pickedType : pickedTypes) {
				if (pickedName.equals(pickedType)) {
					if (ret == null) {
						ret = pickedType;
					} else {
						throw new RuntimeException("Pick name and type error. name: " + name + ", type: " + type);
					}
				}
			}
		}

		return ret;
	}

	public static BaseDB createDB(String name, Params parameter) throws Exception {
		Class <?> cls = dbCLS(name);

		return (BaseDB) cls.getConstructor(Params.class).newInstance(parameter);
	}

	public static Object createOp(String name, IOType type, Params parameter)
		throws Exception {

		Class <?> c = opCLS(name, type);

		if (c == null) {
			return null;
		}

		return c.getConstructor(Params.class).newInstance(parameter);
	}
}
