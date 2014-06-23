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
class TupleGenerator {

	// Parameters for tuple classes	

	private static final String ROOT_DIRECTORY = "./src/main/java";

	private static final String PACKAGE = "eu.stratosphere.api.java.tuple";
	
	private static final String BUILDER_SUFFIX = "builder";

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
	
	// Parameters for JoinOperator
	private static final String JOIN_OPERATOR_PACKAGE = "eu.stratosphere.api.java.operators";
	
	private static final String JOIN_OPERATOR_CLASSNAME = "JoinOperator";

	// parameters for CrossOperator
	private static final String CROSS_OPERATOR_PACKAGE = "eu.stratosphere.api.java.operators";

	private static final String CROSS_OPERATOR_CLASSNAME = "CrossOperator";

	// min. and max. tuple arity	
	private static final int FIRST = 1;

	private static final int LAST = 25;

	public static void main(String[] args) throws Exception {
		File root = new File(ROOT_DIRECTORY);

		createTupleClasses(root);
		
		createTupleBuilderClasses(root);

		modifyCsvReader(root);
		
		modifyTupleTypeInfo(root);
		
		modifyProjectOperator(root);
		
		modifyJoinProjectOperator(root);

		modifyCrossProjectOperator(root);

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

	private static void modifyCrossProjectOperator(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();

		for (int numFields = FIRST; numFields <= LAST; numFields++) {

			// method begin
			sb.append("\n");

			// method comment
			sb.append("\t\t/**\n");
			sb.append("\t\t * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. \n");
			sb.append("\t\t * Requires the classes of the fields of the resulting tuples. \n");
			sb.append("\t\t * \n");
			for (int i = 0; i < numFields; i++) {
				sb.append("\t\t * @param type" + i + " The class of field '"+i+"' of the result tuples.\n");
			}
			sb.append("\t\t * @return The projected data set.\n");
			sb.append("\t\t * \n");
			sb.append("\t\t * @see Tuple\n");
			sb.append("\t\t * @see DataSet\n");
			sb.append("\t\t */\n");

			// method signature
			sb.append("\t\tpublic <");
			appendTupleTypeGenerics(sb, numFields);
			sb.append("> ProjectCross<I1, I2, Tuple"+numFields+"<");
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
			sb.append("\t\t\tif(types.length != this.fieldIndexes.length) {\n");
			sb.append("\t\t\t\tthrow new IllegalArgumentException(\"Numbers of projected fields and types do not match.\");\n");
			sb.append("\t\t\t}\n");
			sb.append("\t\t\t\n");
			sb.append("\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);\n");

			// create new tuple type info
			sb.append("\t\t\tTupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> tType = new TupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(fTypes);\n\n");

			// create and return new project operator
			sb.append("\t\t\treturn new ProjectCross<I1, I2, Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);\n");

			// method end
			sb.append("\t\t}\n");

		}

		// insert code into file
		File dir = getPackage(root, CROSS_OPERATOR_PACKAGE);
		File projectOperatorClass = new File(dir, CROSS_OPERATOR_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), projectOperatorClass);
	}
	
	private static void modifyProjectOperator(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		
		for (int numFields = FIRST; numFields <= LAST; numFields++) {

			// method begin
			sb.append("\n");
			
			// method comment
			sb.append("\t\t/**\n");
			sb.append("\t\t * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. \n");
			sb.append("\t\t * Requires the classes of the fields of the resulting Tuples. \n");
			sb.append("\t\t * \n");			
			for (int i = 0; i < numFields; i++) {
				sb.append("\t\t * @param type" + i + " The class of field '"+i+"' of the result Tuples.\n");
			}
			sb.append("\t\t * @return The projected DataSet.\n");
			sb.append("\t\t * \n");			
			sb.append("\t\t * @see Tuple\n");
			sb.append("\t\t * @see DataSet\n");
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
			sb.append("\t\t\tif(types.length != this.fieldIndexes.length) {\n");
			sb.append("\t\t\t\tthrow new IllegalArgumentException(\"Numbers of projected fields and types do not match.\");\n");
			sb.append("\t\t\t}\n");
			sb.append("\t\t\t\n");
			sb.append("\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());\n");
			
			// create new tuple type info
			sb.append("\t\t\tTupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> tType = new TupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(fTypes);\n\n");
			
			// create and return new project operator
			sb.append("\t\t\treturn new ProjectOperator<T, Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(this.ds, this.fieldIndexes, tType);\n");
			
			// method end
			sb.append("\t\t}\n");
			
		}
		
		// insert code into file
		File dir = getPackage(root, PROJECT_OPERATOR_PACKAGE);
		File projectOperatorClass = new File(dir, PROJECT_OPERATOR_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), projectOperatorClass);
	}
	
	private static void modifyJoinProjectOperator(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		
		for (int numFields = FIRST; numFields <= LAST; numFields++) {

			// method begin
			sb.append("\n");
			
			// method comment
			sb.append("\t\t/**\n");
			sb.append("\t\t * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. \n");
			sb.append("\t\t * Requires the classes of the fields of the resulting tuples. \n");
			sb.append("\t\t * \n");			
			for (int i = 0; i < numFields; i++) {
				sb.append("\t\t * @param type" + i + " The class of field '"+i+"' of the result tuples.\n");
			}
			sb.append("\t\t * @return The projected data set.\n");
			sb.append("\t\t * \n");			
			sb.append("\t\t * @see Tuple\n");
			sb.append("\t\t * @see DataSet\n");
			sb.append("\t\t */\n");
			
			// method signature
			sb.append("\t\tpublic <");
			appendTupleTypeGenerics(sb, numFields);
			sb.append("> ProjectJoin<I1, I2, Tuple"+numFields+"<");
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
			sb.append("\t\t\tif(types.length != this.fieldIndexes.length) {\n");
			sb.append("\t\t\t\tthrow new IllegalArgumentException(\"Numbers of projected fields and types do not match.\");\n");
			sb.append("\t\t\t}\n");
			sb.append("\t\t\t\n");
			sb.append("\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);\n");
			
			// create new tuple type info
			sb.append("\t\t\tTupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">> tType = new TupleTypeInfo<Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(fTypes);\n\n");
			
			// create and return new project operator
			sb.append("\t\t\treturn new ProjectJoin<I1, I2, Tuple"+numFields+"<");
			appendTupleTypeGenerics(sb, numFields);
			sb.append(">>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);\n");
			
			// method end
			sb.append("\t\t}\n");
			
		}
		
		// insert code into file
		File dir = getPackage(root, JOIN_OPERATOR_PACKAGE);
		File projectOperatorClass = new File(dir, JOIN_OPERATOR_CLASSNAME + ".java");
		insertCodeIntoFile(sb.toString(), projectOperatorClass);
	}
	
	private static void modifyTupleTypeInfo(File root) throws IOException {
		// generate code
		StringBuilder sb = new StringBuilder();
		sb.append("\tprivate static final Class<?>[] CLASSES = new Class<?>[] {\n\t\t");
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
		StringBuilder sb = new StringBuilder(1000);
		for (int numFields = FIRST; numFields <= LAST; numFields++) {

			// method begin
			sb.append("\n");
			
			// java doc
			sb.append("\t/**\n");
			sb.append("\t * Specifies the types for the CSV fields. This method parses the CSV data to a ").append(numFields).append("-tuple\n");
			sb.append("\t * which has fields of the specified types.\n");
			sb.append("\t * This method is overloaded for each possible length of the tuples to support type safe\n");
			sb.append("\t * creation of data sets through CSV parsing.\n"); 
			sb.append("\t *\n");
			
			for (int pos = 0; pos < numFields; pos++) {
				sb.append("\t * @param type").append(pos);
				sb.append(" The type of CSV field ").append(pos).append(" and the type of field ");
				sb.append(pos).append(" in the returned tuple type.\n");
			}
			sb.append("\t * @return The {@link eu.stratosphere.api.java.DataSet} representing the parsed CSV data.\n");
			sb.append("\t */\n");

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

		// class declaration and class comments
		w.println("/**");
		w.println(" * A tuple with " + numFields + " fields. Tuples are strongly typed; each field may be of a separate type.");
		w.println(" * The fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position");
		w.println(" * through the {@link #getField(int)} method. The tuple field positions start at zero.");
		w.println(" * <p>");
		w.println(" * Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work");
		w.println(" * with Tuples to reuse objects in order to reduce pressure on the garbage collector.");
		w.println(" *");
		w.println(" * @see Tuple");
		w.println(" *");
		for (int i = 0; i < numFields; i++) {
			w.println(" * @param <" + GEN_TYPE_PREFIX + i + "> The type of field " + i);
		}
		w.println(" */");
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
			w.println("\t\t\t+ \", \" + StringUtils.arrayAwareToString(this.f" + i + ")");
		}
		w.println("\t\t\t+ \")\";");
		w.println("\t}");

		w.println();
		w.println("\t/**");
		w.println("\t * Deep equality for tuples by calling equals() on the tuple members");
		w.println("\t * @param o the object checked for equality");
		w.println("\t * @return true if this is equal to o.");
		w.println("\t */");
		w.println("\t@Override");
		w.println("\tpublic boolean equals(Object o) {");
		w.println("\t\tif(this == o) { return true; }");
		w.println("\t\tif (!(o instanceof " + className + ")) { return false; }");
		w.println("\t\t@SuppressWarnings(\"rawtypes\")");
		w.println("\t\t" + className + " tuple = (" + className + ") o;");
		for (int i = 0; i < numFields; i++) {
			String field = "f" + i;
			w.println("\t\tif (" + field + " != null ? !" + field +".equals(tuple." +
					field + ") : tuple." + field + " != null) { return false; }");
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


		String tupleTypes = "<";
		for (int i = 0; i < numFields; i++) {
			tupleTypes += "T" + i;
			if (i < numFields - 1) {
				tupleTypes += ",";
			}
		}
		tupleTypes += ">";

		w.println();
		w.println("\t/**");
		w.println("\t* Shallow tuple copy.");
		w.println("\t* @returns A new Tuple with the same fields as this.");
		w.println("\t */");
		w.println("\tpublic " + className + tupleTypes + " copy(){ ");

		w.print("\t\treturn new " + className + tupleTypes + "(this.f0");
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

		// foot
		w.println("}");
	}
	
	private static void createTupleBuilderClasses(File root) throws FileNotFoundException {
		File dir = getPackage(root, PACKAGE+"."+BUILDER_SUFFIX);

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
		w.println("import java.util.LinkedList;");
		w.println("import java.util.List;");
		w.println();
		w.println("import " + PACKAGE + ".Tuple" + numFields + ";");
		w.println();

		// class declaration
		w.print("public class " + className);
		printGenericsString(w, numFields);
		w.println(" {");
		w.println();

		// Class-Attributes - a list of tuples
		w.print("\tprivate List<Tuple" + numFields);
		printGenericsString(w, numFields);
		w.print("> tuples = new LinkedList<Tuple" + numFields );
		printGenericsString(w, numFields);
		w.println(">();");
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
		w.print("\t\ttuples.add(new Tuple" + numFields);
		printGenericsString(w, numFields);
		w.print("(");
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
	
	private static String HEADER = 
		"/***********************************************************************************************************************\n" +
		" *\n" +
		" * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)\n" +
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
