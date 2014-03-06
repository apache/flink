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
import java.io.PrintWriter;

/**
 * Source code generator for tuple classes.
 */
public class TupleGenerator {
	
	private static final String ROOT_DIRECTORY = "./src/main/java/";
	
	private static final String PACKAGE = "eu.stratosphere.api.java.tuple";
	
	private static final String GEN_TYPE_PREFIX = "T";
	
	private static final int FIRST = 1;
	
	private static final int LAST = 22;
	
	
	public static void main(String[] args) throws Exception {
		File root = new File(ROOT_DIRECTORY);
		File dir = new File(root, PACKAGE.replace('.', '/'));
		if (!dir.exists() && dir.isDirectory()) {
			System.err.println("None existent directory: " + dir.getAbsolutePath());
			System.exit(1);
		}
		
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
		w.println("@SuppressWarnings({\"restriction\"})");;
		w.print("public class " + className + "<");
		for (int i = 1; i <= numFields; i++) {
			if (i > 1) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i);
		}
		w.println("> extends Tuple {");
		w.println();
		
		w.println("\tprivate static final long serialVersionUID = 1L;");
		w.println();
		
		// fields
		for (int i = 1; i <= numFields; i++) {
			w.println("\tprivate " + GEN_TYPE_PREFIX + i + " _" + i + ';');
		}
		w.println();
		
		// constructors
		w.println("\tpublic " + className + "() {}");
		w.println();
		w.print("\tpublic " + className + "(");
		for (int i = 1; i <= numFields; i++) {
			if (i > 1) {
				w.print(", ");
			}
			w.print(GEN_TYPE_PREFIX + i + " value" + i);
		}
		w.println(") {");
		for (int i = 1; i <= numFields; i++) {
			w.println("\t\tthis._" + i + " = value" + i + ';');
		}		
		w.println("\t}");
		w.println();
		
		// individual accessors
		for (int i = 1; i <= numFields; i++) {
			w.println("\tpublic " + GEN_TYPE_PREFIX + i + " " + GEN_TYPE_PREFIX + i + "() {");
			w.println("\t\treturn this._" + i + ";");
			w.println("\t}");
		}
		
		for (int i = 1; i <= numFields; i++) {
			w.println("\tpublic void " + GEN_TYPE_PREFIX + i + "(" + GEN_TYPE_PREFIX + i + " value) {");
			w.println("\t\tthis._" + i + " = value;");
			w.println("\t}");
		}
		
		
		// accessor getter method
		w.println("\t@Override");
		w.println("\t@SuppressWarnings(\"unchecked\")");
		w.println("\tpublic <T> T getField(int pos) {");
		w.println("\t\tswitch(pos) {");
		for (int i = 0; i < numFields; i++) {
			w.println("\t\t\tcase " + i + ": return (T) this._" + (i+1) + ';');
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
			w.println("\t\t\t\tthis._" + (i+1) + " = (" + GEN_TYPE_PREFIX + (i+1) + ") value;");
			w.println("\t\t\t\tbreak;");
		}
		w.println("\t\t\tdefault: throw new IndexOutOfBoundsException(String.valueOf(pos));");
		w.println("\t\t}");
		w.println("\t}");
		
		// standard utilities (toString, equals, hashCode)
		w.println();
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println("\t// standard utilities");
		w.println("\t// -------------------------------------------------------------------------------------------------");
		w.println();
		w.println("\t@Override");
		w.println("\tpublic String toString() {");
		w.println("\t\treturn \"(\" + StringUtils.arrayAwareToString(this._1)");
		for (int i = 2; i <= numFields; i++) {
			w.println("\t\t\t+ \", \" + StringUtils.arrayAwareToString(this._" + i + ")");
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
			w.println("\t\t\toffsets[" + i + "] = UNSAFE.objectFieldOffset(Tuple" + numFields + ".class.getDeclaredField(\"_" + (i+1) + "\"));");
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
