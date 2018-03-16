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

package org.apache.flink.api.java.tuple;

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 * Source code generator for tuple classes and classes which depend on the arity of tuples.
 *
 * <p>This class is responsible for generating the various {@link Tuple} and TupleBuilder classes.
 */
class TupleGenerator {

	// Parameters for tuple classes

	private static final String ROOT_DIRECTORY = "./flink-core/src/main/java";

	private static final String PACKAGE = "org.apache.flink.api.java.tuple";

	private static final String BUILDER_SUFFIX = "builder";

	private static final String GEN_TYPE_PREFIX = "T";

	// Parameters for tuple-dependent classes
	private static final String BEGIN_INDICATOR = "BEGIN_OF_TUPLE_DEPENDENT_CODE";

	private static final String END_INDICATOR = "END_OF_TUPLE_DEPENDENT_CODE";

	// Parameters for TupleTypeInfo
	private static final String TUPLE_PACKAGE = "org.apache.flink.api.java.tuple";

	private static final String TUPLE_CLASSNAME = "Tuple";

	// min. and max. tuple arity
	private static final int FIRST = 1;

	private static final int LAST = 25;

	public static void main(String[] args) throws Exception {
		System.err.println("Current directory " + System.getProperty("user.dir"));
		String rootDir = ROOT_DIRECTORY;
		if (args.length > 0) {
			rootDir = args[0] + "/" + ROOT_DIRECTORY;
		}
		System.err.println("Using root directory: " + rootDir);
		File root = new File(rootDir);

		createTupleClasses(root);

		createTupleBuilderClasses(root);

		modifyTupleType(root);

	}

	private static File getPackage(File root, String packageString) {
		File dir = new File(root, packageString.replace('.', '/'));
		if (!dir.exists() && dir.isDirectory()) {
			System.err.println("None existent directory: " + dir.getAbsolutePath());
			System.exit(1);
		}
		return dir;
	}

	private static void insertCodeIntoFile(String code, File file) throws IOException {
		String fileContent = FileUtils.readFileUtf8(file);

		try (Scanner s = new Scanner(fileContent)) {
			StringBuilder sb = new StringBuilder();
			String line;

			boolean indicatorFound = false;

			// add file beginning
			while (s.hasNextLine() && (line = s.nextLine()) != null) {
				sb.append(line).append("\n");
				if (line.contains(BEGIN_INDICATOR)) {
					indicatorFound = true;
					break;
				}
			}

			if (!indicatorFound) {
				System.out.println("No indicator found in '" + file + "'. Will skip code generation.");
				s.close();
				return;
			}

			// add generator signature
			sb.append("\t// GENERATED FROM ").append(TupleGenerator.class.getName()).append(".\n");

			// add tuple dependent code
			sb.append(code).append("\n");

			// skip generated code
			while (s.hasNextLine() && (line = s.nextLine()) != null) {
				if (line.contains(END_INDICATOR)) {
					sb.append(line).append("\n");
					break;
				}
			}

			// add file ending
			while (s.hasNextLine() && (line = s.nextLine()) != null) {
				sb.append(line).append("\n");
			}
			FileUtils.writeFileUtf8(file, sb.toString());
		}
	}

	private static void modifyTupleType(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		sb.append("\tprivate static final Class<?>[] CLASSES = new Class<?>[] {\n\t\tTuple0.class");
		for (int i = FIRST; i <= LAST; i++) {
			sb.append(", Tuple").append(i).append(".class");
		}
		sb.append("\n\t};");

		// insert code into file
		File dir = getPackage(root, TUPLE_PACKAGE);
		File tupleTypeInfoClass = new File(dir, TUPLE_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), tupleTypeInfoClass);
	}

	private static void createTupleClasses(File root) throws FileNotFoundException {
		File dir = getPackage(root, PACKAGE);

		for (int i = FIRST; i <= LAST; i++) {
			File tupleFile = new File(dir, "Tuple" + i + ".java");
				PrintWriter writer = new PrintWriter(tupleFile);
			writeTupleClass(writer, i);
			writer.flush();
			writer.close();
		}
	}

	private static void writeTupleClass(PrintWriter w, int numFields) {
		final String className = "Tuple" + numFields;

		// head
		w.print(HEADER);

		// package and imports
		w.println("package " + PACKAGE + ';');
		w.println();
		w.println("import org.apache.flink.annotation.Public;");
		w.println("import org.apache.flink.util.StringUtils;");
		w.println();

		// class declaration and class comments
		w.println("/**");
		w.println(" * A tuple with " + numFields + " fields. Tuples are strongly typed; each field may be of a separate type.");
		w.println(" * The fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position");
		w.println(" * through the {@link #getField(int)} method. The tuple field positions start at zero.");
		w.println(" *");
		w.println(" * <p>Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work");
		w.println(" * with Tuples to reuse objects in order to reduce pressure on the garbage collector.</p>");
		w.println(" *");
		w.println(" * <p>Warning: If you subclass " + className + ", then be sure to either <ul>");
		w.println(" *  <li> not add any new fields, or </li>");
		w.println(" *  <li> make it a POJO, and always declare the element type of your DataStreams/DataSets to your descendant");
		w.println(" *       type. (That is, if you have a \"class Foo extends " + className + "\", then don't use instances of");
		w.println(" *       Foo in a DataStream&lt;" + className + "&gt; / DataSet&lt;" + className + "&gt;, but declare it as");
		w.println(" *       DataStream&lt;Foo&gt; / DataSet&lt;Foo&gt;.) </li>");
		w.println(" * </ul></p>");
		w.println(" * @see Tuple");
		w.println(" *");
		for (int i = 0; i < numFields; i++) {
			w.println(" * @param <" + GEN_TYPE_PREFIX + i + "> The type of field " + i);
		}
		w.println(" */");
		w.println("@Public");
		w.print("public class " + className + "<");
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i);
		}
		w.println("> extends Tuple {");
		w.println();

		w.println("\tprivate static final long serialVersionUID = 1L;");
		w.println();

		// fields
		for (int i = 0; i < numFields; i++) {
			w.println("\t/** Field " + i + " of the tuple. */");
			w.println("\tpublic " + GEN_TYPE_PREFIX + i + " f" + i + ';');
		}
		w.println();

		String paramList = "("; // This will be like "(T0 value0, T1 value1)"
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				paramList += ", ";
			}
			paramList += GEN_TYPE_PREFIX + i + " value" + i;
		}
		paramList += ")";

		// constructors
		w.println("\t/**");
		w.println("\t * Creates a new tuple where all fields are null.");
		w.println("\t */");
		w.println("\tpublic " + className + "() {}");
		w.println();
		w.println("\t/**");
		w.println("\t * Creates a new tuple and assigns the given values to the tuple's fields.");
		w.println("\t *");
		for (int i = 0; i < numFields; i++) {
			w.println("\t * @param value" + i + " The value for field " + i);
		}
		w.println("\t */");
		w.println("\tpublic " + className + paramList + " {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\tthis.f" + i + " = value" + i + ';');
		}
		w.println("\t}");
		w.println();

		// arity accessor
		w.println("\t@Override");
		w.println("\tpublic int getArity() {");
		w.println("\t\treturn " + numFields + ";");
		w.println("\t}");
		w.println();

		// accessor getter method
		w.println("\t@Override");
		w.println("\t@SuppressWarnings(\"unchecked\")");
		w.println("\tpublic <T> T getField(int pos) {");
		w.println("\t\tswitch(pos) {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\t\tcase " + i + ": return (T) this.f" + i + ';');
		}
		w.println("\t\t\tdefault: throw new IndexOutOfBoundsException(String.valueOf(pos));");
		w.println("\t\t}");
		w.println("\t}");
		w.println();

		// accessor setter method
		w.println("\t@Override");
		w.println("\t@SuppressWarnings(\"unchecked\")");
		w.println("\tpublic <T> void setField(T value, int pos) {");
		w.println("\t\tswitch(pos) {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\t\tcase " + i + ':');
			w.println("\t\t\t\tthis.f" + i + " = (" + GEN_TYPE_PREFIX + i + ") value;");
			w.println("\t\t\t\tbreak;");
		}
		w.println("\t\t\tdefault: throw new IndexOutOfBoundsException(String.valueOf(pos));");
		w.println("\t\t}");
		w.println("\t}");
		w.println();

		// accessor setter method for all fields
		w.println("\t/**");
		w.println("\t * Sets new values to all fields of the tuple.");
		w.println("\t *");
		for (int i = 0; i < numFields; i++) {
			w.println("\t * @param value" + i + " The value for field " + i);
		}
		w.println("\t */");
		w.println("\tpublic void setFields" + paramList + " {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\tthis.f" + i + " = value" + i + ';');
		}
		w.println("\t}");
		w.println();

		// swap method only for Tuple2
		if (numFields == 2) {
			w.println("\t/**");
			w.println("\t* Returns a shallow copy of the tuple with swapped values.");
			w.println("\t*");
			w.println("\t* @return shallow copy of the tuple with swapped values");
			w.println("\t*/");
			w.println("\tpublic Tuple2<T1, T0> swap() {");
			w.println("\t\treturn new Tuple2<T1, T0>(f1, f0);");
			w.println("\t}");
		}

		// standard utilities (toString, equals, hashCode, copy)
		w.println();
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println("\t// standard utilities");
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println();
		w.println("\t/**");
		w.println("\t * Creates a string representation of the tuple in the form");
		w.print("\t * (f0");
		for (int i = 1; i < numFields; i++) {
			w.print(", f" + i);
		}
		w.println("),");
		w.println("\t * where the individual fields are the value returned by calling {@link Object#toString} on that field.");
		w.println("\t * @return The string representation of the tuple.");
		w.println("\t */");
		w.println("\t@Override");
		w.println("\tpublic String toString() {");
		w.println("\t\treturn \"(\" + StringUtils.arrayAwareToString(this.f0)");
		for (int i = 1; i < numFields; i++) {
			w.println("\t\t\t+ \",\" + StringUtils.arrayAwareToString(this.f" + i + ")");
		}
		w.println("\t\t\t+ \")\";");
		w.println("\t}");

		w.println();
		w.println("\t/**");
		w.println("\t * Deep equality for tuples by calling equals() on the tuple members.");
		w.println("\t * @param o the object checked for equality");
		w.println("\t * @return true if this is equal to o.");
		w.println("\t */");
		w.println("\t@Override");
		w.println("\tpublic boolean equals(Object o) {");
		w.println("\t\tif (this == o) {");
		w.println("\t\t\treturn true;");
		w.println("\t\t}");
		w.println("\t\tif (!(o instanceof " + className + ")) {");
		w.println("\t\t\treturn false;");
		w.println("\t\t}");
		w.println("\t\t@SuppressWarnings(\"rawtypes\")");
		w.println("\t\t" + className + " tuple = (" + className + ") o;");
		for (int i = 0; i < numFields; i++) {
			String field = "f" + i;
			w.println("\t\tif (" + field + " != null ? !" + field + ".equals(tuple." +
					field + ") : tuple." + field + " != null) {");
			w.println("\t\t\treturn false;");
			w.println("\t\t}");
		}
		w.println("\t\treturn true;");
		w.println("\t}");

		w.println();
		w.println("\t@Override");
		w.println("\tpublic int hashCode() {");
		w.println("\t\tint result = f0 != null ? f0.hashCode() : 0;");
		for (int i = 1; i < numFields; i++) {
			String field = "f" + i;
			w.println("\t\tresult = 31 * result + (" + field + " != null ? " + field + ".hashCode() : 0);");
		}
		w.println("\t\treturn result;");
		w.println("\t}");

		String tupleTypes = "";
		for (int i = 0; i < numFields; i++) {
			tupleTypes += "T" + i;
			if (i < numFields - 1) {
				tupleTypes += ", ";
			}
		}

		w.println();
		w.println("\t/**");
		w.println("\t* Shallow tuple copy.");
		w.println("\t* @return A new Tuple with the same fields as this.");
		w.println("\t*/");
		w.println("\t@Override");
		w.println("\t@SuppressWarnings(\"unchecked\")");
		w.println("\tpublic " + className + "<" + tupleTypes + "> copy() {");

		w.print("\t\treturn new " + className + "<>(this.f0");
		if (numFields > 1) {
			w.println(",");
		}
		for (int i = 1; i < numFields; i++) {
			String field = "f" + i;
			w.print("\t\t\tthis." + field);
			if (i < numFields - 1) {
				w.println(",");
			}
		}
		w.println(");");
		w.println("\t}");

		w.println();
		w.println("\t/**");
		w.println("\t * Creates a new tuple and assigns the given values to the tuple's fields.");
		w.println("\t * This is more convenient than using the constructor, because the compiler can");
		w.println("\t * infer the generic type arguments implicitly. For example:");
		w.println("\t * {@code Tuple3.of(n, x, s)}");
		w.println("\t * instead of");
		w.println("\t * {@code new Tuple3<Integer, Double, String>(n, x, s)}");
		w.println("\t */");
		w.println("\tpublic static <" + tupleTypes + "> " + className + "<" + tupleTypes + "> of" + paramList + " {");

		w.print("\t\treturn new " + className + "<>(value0");
		if (numFields > 1) {
			w.println(",");
		}
		for (int i = 1; i < numFields; i++) {
			w.print("\t\t\tvalue" + i);
			if (i < numFields - 1) {
				w.println(",");
			}
		}
		w.println(");");
		w.println("\t}");

		// foot
		w.println("}");
	}

	private static void createTupleBuilderClasses(File root) throws FileNotFoundException {
		File dir = getPackage(root, PACKAGE + "." + BUILDER_SUFFIX);

		for (int i = FIRST; i <= LAST; i++) {
			File tupleFile = new File(dir, "Tuple" + i + "Builder.java");
			PrintWriter writer = new PrintWriter(tupleFile);
			writeTupleBuilderClass(writer, i);
			writer.flush();
			writer.close();
		}
	}

	private static void printGenericsString(PrintWriter w, int numFields){
		w.print("<");
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i);
		}
		w.print(">");
	}

	private static void writeTupleBuilderClass(PrintWriter w, int numFields) {
		final String className = "Tuple" + numFields + "Builder";

		// head
		w.print(HEADER);

		// package and imports
		w.println("package " + PACKAGE + "." + BUILDER_SUFFIX + ';');
		w.println();
		w.println("import org.apache.flink.annotation.Public;");
		w.println("import " + PACKAGE + ".Tuple" + numFields + ";");
		w.println();
		w.println("import java.util.ArrayList;");
		w.println("import java.util.List;");
		w.println();

		// class javadoc
		w.println("/**");
		w.println(" * A builder class for {@link Tuple" + numFields + "}.");
		w.println(" *");
		for (int i = 0; i < numFields; i++) {
			w.println(" * @param <" + GEN_TYPE_PREFIX + i + "> The type of field " + i);
		}
		w.println(" */");

		// class declaration
		w.println("@Public");
		w.print("public class " + className);
		printGenericsString(w, numFields);
		w.println(" {");
		w.println();

		// Class-Attributes - a list of tuples
		w.print("\tprivate List<Tuple" + numFields);
		printGenericsString(w, numFields);
		w.println("> tuples = new ArrayList<>();");
		w.println();

		// add(...) function for adding a single tuple
		w.print("\tpublic " + className);
		printGenericsString(w, numFields);
		w.print(" add(");
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i + " value" + i);
		}
		w.println("){");
		w.print("\t\ttuples.add(new Tuple" + numFields + "<>(");
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				w.print(", ");
			}
			w.print("value" + i);
		}
		w.println("));");
		w.println("\t\treturn this;");
		w.println("\t}");
		w.println();

		// build() function, returns an array of tuples
		w.println("\t@SuppressWarnings(\"unchecked\")");
		w.print("\tpublic Tuple" + numFields);
		printGenericsString(w, numFields);
		w.println("[] build(){");
		w.println("\t\treturn tuples.toArray(new Tuple" + numFields + "[tuples.size()]);");
		w.println("\t}");

		// foot
		w.println("}");
	}

	private static final String HEADER =
		"/*\n"
		+ " * Licensed to the Apache Software Foundation (ASF) under one\n"
		+ " * or more contributor license agreements.  See the NOTICE file\n"
		+ " * distributed with this work for additional information\n"
		+ " * regarding copyright ownership.  The ASF licenses this file\n"
		+ " * to you under the Apache License, Version 2.0 (the\n"
		+ " * \"License\"); you may not use this file except in compliance\n"
		+ " * with the License.  You may obtain a copy of the License at\n"
		+ " *\n"
		+ " *     http://www.apache.org/licenses/LICENSE-2.0\n"
		+ " *\n"
		+ " * Unless required by applicable law or agreed to in writing, software\n"
		+ " * distributed under the License is distributed on an \"AS IS\" BASIS,\n"
		+ " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
		+ " * See the License for the specific language governing permissions and\n"
		+ " * limitations under the License.\n"
		+ " */" +
		"\n" +
		"\n" +
		"// --------------------------------------------------------------\n" +
		"//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!\n" +
		"//  GENERATED FROM " + TupleGenerator.class.getName() + ".\n" +
		"// --------------------------------------------------------------\n\n";
}
