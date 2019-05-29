package org.apache.flink.formats.thrift.typeutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility methods.
 */
public class Utils {

	public static List<String> getClassAndTypeArguments(String genericClassName) {
		genericClassName = genericClassName.trim();
		List<String> classNames = new ArrayList<>();

		if (!genericClassName.contains("<")) {
			classNames.add(genericClassName);
			return classNames;
		}

		String[] splits = genericClassName.split(">")[0].split("<");
		String mainClassName = splits[0];
		String[] typeArgs = splits[1].split(",");
		classNames.add(mainClassName);
		classNames.addAll(Arrays.asList(typeArgs));
		return classNames;
	}

	public static boolean isAssignableFrom(Class superClass, Class clazz) throws IOException {
		return isAssignableFrom(superClass, clazz.getName());
	}

	public static boolean isAssignableFrom(Class superClass, String className) throws IOException {
		Class clazz;
		try {
			clazz = Class.forName(className);
			return superClass.isAssignableFrom(clazz);
		} catch (ClassNotFoundException e) {
			List<String> decomposed = getClassAndTypeArguments(className);
			try {
				clazz = Class.forName(decomposed.get(0));
				return superClass.isAssignableFrom(clazz);
			} catch (ClassNotFoundException e2) {
				throw new IOException(e2);
			}
		}
	}

}
