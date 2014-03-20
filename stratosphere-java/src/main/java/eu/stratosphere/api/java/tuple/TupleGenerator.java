/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.api.java.tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Source code generator for tuple classes and classes which depend on the arity
 * of tuples.
 */
public class TupleGenerator {

	// Parameters for tuple classes	
	private static final String ROOT_DIRECTORY = "./src/main/java";

	private static final String PACKAGE = "eu.stratosphere.api.java.tuple";

	private static final String GEN_TYPE_PREFIX = "T";

	// Parameters for tuple-dependent classes
	private static final String BEGIN_INDICATOR = "BEGIN_OF_TUPLE_DEPENDENT_CODE";

	private static final String END_INDICATOR = "END_OF_TUPLE_DEPENDENT_CODE";

	// Parameters for CsvReader	
	private static final String CSV_READER_PACKAGE = "eu.stratosphere.api.java.io";

	private static final String CSV_READER_CLASSNAME = "CsvReader";
	
	// Parameters for TupleTypeInfo
	private static final String TUPLE_TYPE_INFO_PACKAGE = "eu.stratosphere.api.java.typeutils";
	
	private static final String TUPLE_TYPE_INFO_CLASSNAME = "TupleTypeInfo";
	
	// Parameters for ProjectOperator
	private static final String PROJECT_OPERATOR_PACKAGE = "eu.stratosphere.api.java.operators";
	
	private static final String PROJECT_OPERATOR_CLASSNAME = "ProjectOperator";

	// min. and max. tuple arity	
	private static final int FIRST = 1;

	private static final int LAST = 22;

	public static void main(String[] args) throws Exception {
		File root = new File(ROOT_DIRECTORY);

		createTupleClasses(root);

		modifyCsvReader(root);
		
		modifyTupleTypeInfo(root);
		
		modifyProjectOperator(root);
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
		String fileContent = Files.toString(file, Charsets.UTF_8);
		Scanner s = new Scanner(fileContent);

		StringBuilder sb = new StringBuilder();
		String line = null;
		
		boolean indicatorFound = false;

		// add file beginning
		while (s.hasNextLine() && (line = s.nextLine()) != null) {
			sb.append(line + "\n");
			if (line.contains(BEGIN_INDICATOR)) {
				indicatorFound = true;
				break;
			}
		}
		
		if(!indicatorFound) {
			System.out.println("No indicator found in '" + file + "'. Will skip code generation.");
			s.close();
			return;
		}

		// add generator signature
		sb.append("\t// GENERATED FROM " + TupleGenerator.class.getName() + ".\n");

		// add tuple dependent code
		sb.append(code + "\n");

		// skip generated code
		while (s.hasNextLine() && (line = s.nextLine()) != null) {
			if (line.contains(END_INDICATOR)) {
				sb.append(line + "\n");
				break;
			}
		}

		// add file ending
		while (s.hasNextLine() && (line = s.nextLine()) != null) {
			sb.append(line + "\n");
		}
		s.close();
		Files.write(sb.toString(), file, Charsets.UTF_8);
	}
	
	private static void modifyProjectOperator(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		
		for (int numFields = FIRST; numFields <= LAST; numFields++) {

			// method begin
			sb.append("\n");
			
			// method comment
			sb.append("\t\t/**\n");
			sb.append("\t\t * Projects a tuple data set to the previously selected fields. \n");
			sb.append("\t\t * Requires the classes of the fields of the resulting tuples. \n");
			sb.append("\t\t * \n");			
			for (int i = 0; i < numFields; i++) {
				sb.append("\t\t * @param type" + i + " The class of field '"+i+"' of the result tuples.\n");
			}
			sb.append("\t\t * @return The projected data set.\n");
			sb.append("\t\t */\n");
			
			// method signature
			sb.append("\t\tpublic <");
			appendTupleTypeGenerics(sb, numFields);
			sb.append("> ProjectOperator<T, Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> types(");
			for (int i = 0; i < numFields; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("Class<");
				sb.append(GEN_TYPE_PREFIX + i);
				sb.append("> type" + i);
			}
			sb.append(") {\n");
			
			// convert type0..1 to types array
			sb.append("\t\t\tClass<?>[] types = {");
			for (int i = 0; i < numFields; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("type" + i);
			}
			sb.append("};\n");
			
			// check number of types and extract field types
			sb.append("\t\t\tif(types.length != this.fields.length) {\n");
			sb.append("\t\t\t\tthrow new IllegalArgumentException(\"Numbers of projected fields and types do not match.\");\n");
			sb.append("\t\t\t}\n");
			sb.append("\t\t\t\n");
			sb.append("\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());\n");
			
			// create new tuple type info
			sb.append("\t\t\tTupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> tType = new TupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(fTypes);\n\n");
			
			// create and return new project operator
			sb.append("\t\t\treturn new ProjectOperator<T, Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(this.ds, this.fields, tType);\n");
			
			// method end
			sb.append("\t\t}\n");
			
		}
		
		// insert code into file
		File dir = getPackage(root, PROJECT_OPERATOR_PACKAGE);
		File projectOperatorClass = new File(dir, PROJECT_OPERATOR_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), projectOperatorClass);
	}
	
	private static void modifyTupleTypeInfo(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		sb.append("\tprivate static final Class<?>[] CLASSES = new Class<?>[] {\n\t");
		for (int i = FIRST; i <= LAST; i++) {
			if (i > FIRST) {
				sb.append(", ");
			}
			sb.append("Tuple" + i + ".class");
		}
		sb.append("\n\t};");
		
		// insert code into file
		File dir = getPackage(root, TUPLE_TYPE_INFO_PACKAGE);
		File tupleTypeInfoClass = new File(dir, TUPLE_TYPE_INFO_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), tupleTypeInfoClass);
	}

	private static void modifyCsvReader(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		for (int numFields = FIRST; numFields <= LAST; numFields++) {

			// method begin
			sb.append("\n");

			// method signature
			sb.append("\tpublic <");
			appendTupleTypeGenerics(sb, numFields);
			sb.append("> DataSource<Tuple" + numFields + "<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> types(");
			for (int i = 0; i < numFields; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("Class<");
				sb.append(GEN_TYPE_PREFIX + i);
				sb.append("> type" + i);
			}
			sb.append(") {\n");

			// get TupleTypeInfo
			sb.append("\t\tTupleTypeInfo<Tuple" + numFields + "<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> types = TupleTypeInfo.getBasicTupleTypeInfo(");
			for (int i = 0; i < numFields; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("type" + i);
			}
			sb.append(");\n");

			// create csv input format
			sb.append("\t\tCsvInputFormat<Tuple" + numFields + "<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> inputFormat = new CsvInputFormat<Tuple" + numFields + "<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(path);\n");

			// configure input format
			sb.append("\t\tconfigureInputFormat(inputFormat, ");
			for (int i = 0; i < numFields; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("type" + i);
			}
			sb.append(");\n");

			// return
			sb.append("\t\treturn new DataSource<Tuple" + numFields + "<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(executionContext, inputFormat, types);\n");

			// end of method
			sb.append("\t}\n");
		}

		// insert code into file
		File dir = getPackage(root, CSV_READER_PACKAGE);
		File csvReaderClass = new File(dir, CSV_READER_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), csvReaderClass);
	}

	private static void appendTupleTypeGenerics(StringBuilder sb, int numFields) {
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(GEN_TYPE_PREFIX + i);
		}
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
		w.println("import eu.stratosphere.util.StringUtils;");
		w.println();

		// class declaration
		w.println("@SuppressWarnings({\"restriction\"})");
		;
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
			w.println("\tpublic " + GEN_TYPE_PREFIX + i + " f" + i + ';');
		}
		w.println();

		// constructors
		w.println("\tpublic " + className + "() {}");
		w.println();
		w.print("\tpublic " + className + "(");
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i + " value" + i);
		}
		w.println(") {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\tthis.f" + i + " = value" + i + ';');
		}
		w.println("\t}");
		w.println();

		// arity accessor
		w.println("\t@Override");
		w.println("\tpublic int getArity() { return " + numFields + "; }");
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

		// accessor setter method for all fields
		w.print("\tpublic void setFields(");
		for (int i = 0; i < numFields; i++) {
			if (i > 0) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i + " value" + i);
		}
		w.println(") {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\tthis.f" + i + " = value" + i + ';');
		}
		w.println("\t}");
		w.println();

		// standard utilities (toString, equals, hashCode)
		w.println();
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println("\t// standard utilities");
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println();
		w.println("\t@Override");
		w.println("\tpublic String toString() {");
		w.println("\t\treturn \"(\" + StringUtils.arrayAwareToString(this.f0)");
		for (int i = 1; i < numFields; i++) {
			w.println("\t\t\t+ \", \" + StringUtils.arrayAwareToString(this.f" + i + ")");
		}
		w.println("\t\t\t+ \")\";");
		w.println("\t}");

		// unsafe fast field access
		w.println();
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println("\t// unsafe fast field access");
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println();
		w.println("\t@SuppressWarnings({ \"unchecked\"})");
		w.println("\tpublic <T> T getFieldFast(int pos) {");
		w.println("\t\treturn (T) UNSAFE.getObject(this, offsets[pos]);");
		w.println("\t}");
		w.println();
		w.println("\tprivate static final sun.misc.Unsafe UNSAFE = eu.stratosphere.core.memory.MemoryUtils.UNSAFE;");
		w.println();
		w.println("\tprivate static final long[] offsets = new long[" + numFields + "];");
		w.println();
		w.println("\tstatic {");
		w.println("\t\ttry {");

		for (int i = 0; i < numFields; i++) {
			w.println("\t\t\toffsets[" + i + "] = UNSAFE.objectFieldOffset(Tuple" + numFields + ".class.getDeclaredField(\"f" + i + "\"));");
		}

		w.println("\t\t} catch (Throwable t) {");
		w.println("\t\t\tthrow new RuntimeException(\"Could not initialize fast field accesses for tuple data type.\");");
		w.println("\t\t}");
		w.println("\t}");

		// foot
		w.println("}");
	}

	private static String HEADER = 
		"/***********************************************************************************************************************\n" +
		" *\n" +
		" * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)\n" +
		" *\n" +
		" * Licensed under the Apache License, Version 2.0 (the \"License\"); you may not use this file except in compliance with\n" +
		" * the License. You may obtain a copy of the License at\n" +
		" *\n" +
		" *     http://www.apache.org/licenses/LICENSE-2.0\n" +
		" *\n" +
		" * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on\n" +
		" * an \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the\n" +
		" * specific language governing permissions and limitations under the License.\n" +
		" *\n" +
		" **********************************************************************************************************************/\n" +
		"\n" +
		"// --------------------------------------------------------------\n" +
		"//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!\n" +
		"//  GENERATED FROM " + TupleGenerator.class.getName() + ".\n" +
		"// --------------------------------------------------------------\n\n\n";
}
