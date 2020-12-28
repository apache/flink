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
import java.io.IOException;
import java.util.Scanner;

/**
 * Source code generator for tuple classes and classes which depend on the arity of tuples.
 *
 * <p>This class is responsible for generating tuple-size dependent code in the {@link
 * org.apache.flink.api.java.io.CsvReader}, {@link
 * org.apache.flink.api.java.operators.ProjectOperator}, {@link
 * org.apache.flink.api.java.operators.JoinOperator.JoinProjection} and {@link
 * org.apache.flink.api.java.operators.CrossOperator.CrossProjection}.
 */
class TupleGenerator {

    // Parameters for tuple classes

    private static final String ROOT_DIRECTORY = "./flink-java/src/main/java";

    private static final String GEN_TYPE_PREFIX = "T";

    // Parameters for tuple-dependent classes
    private static final String BEGIN_INDICATOR = "BEGIN_OF_TUPLE_DEPENDENT_CODE";

    private static final String END_INDICATOR = "END_OF_TUPLE_DEPENDENT_CODE";

    // Parameters for CsvReader
    private static final String CSV_READER_PACKAGE = "org.apache.flink.api.java.io";

    private static final String CSV_READER_CLASSNAME = "CsvReader";

    // Parameters for ProjectOperator
    private static final String PROJECT_OPERATOR_PACKAGE = "org.apache.flink.api.java.operators";

    private static final String PROJECT_OPERATOR_CLASSNAME = "ProjectOperator";

    // Parameters for JoinOperator
    private static final String JOIN_OPERATOR_PACKAGE = "org.apache.flink.api.java.operators";

    private static final String JOIN_OPERATOR_CLASSNAME = "JoinOperator";

    // parameters for CrossOperator
    private static final String CROSS_OPERATOR_PACKAGE = "org.apache.flink.api.java.operators";

    private static final String CROSS_OPERATOR_CLASSNAME = "CrossOperator";

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

        modifyCsvReader(root);

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
                System.out.println(
                        "No indicator found in '" + file + "'. Will skip code generation.");
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
            s.close();
            FileUtils.writeFileUtf8(file, sb.toString());
        }
    }

    private static void modifyCrossProjectOperator(File root) throws IOException {
        // generate code
        StringBuilder sb = new StringBuilder();

        // method begin
        sb.append("\n");

        // method comment
        sb.append("\t\t/**\n");
        sb.append("\t\t * Chooses a projectTupleX according to the length of\n");
        sb.append(
                "\t\t * {@link org.apache.flink.api.java.operators.CrossOperator.CrossProjection#fieldIndexes} \n");
        sb.append("\t\t * \n");
        sb.append("\t\t * @return The projected DataSet.\n");
        sb.append("\t\t */\n");

        // method signature
        sb.append("\t\t@SuppressWarnings(\"unchecked\")\n");
        sb.append("\t\tpublic <OUT extends Tuple> ProjectCross<I1, I2, OUT> projectTupleX() {\n");
        sb.append("\t\t\tProjectCross<I1, I2, OUT> projectionCross = null;\n\n");
        sb.append("\t\t\tswitch (fieldIndexes.length) {\n");
        for (int numFields = FIRST; numFields <= LAST; numFields++) {
            sb.append(
                    "\t\t\tcase "
                            + numFields
                            + ":"
                            + " projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple"
                            + numFields
                            + "(); break;\n");
        }
        sb.append(
                "\t\t\tdefault: throw new IllegalStateException(\"Excessive arity in tuple.\");\n");
        sb.append("\t\t\t}\n\n");
        sb.append("\t\t\treturn projectionCross;\n");

        // method end
        sb.append("\t\t}\n");

        for (int numFields = FIRST; numFields <= LAST; numFields++) {

            // method begin
            sb.append("\n");

            // method comment
            sb.append("\t\t/**\n");
            sb.append(
                    "\t\t * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. \n");
            sb.append("\t\t * \n");
            sb.append("\t\t * @return The projected data set.\n");
            sb.append("\t\t * \n");
            sb.append("\t\t * @see Tuple\n");
            sb.append("\t\t * @see DataSet\n");
            sb.append("\t\t */\n");

            // method signature
            sb.append("\t\tpublic <");
            appendTupleTypeGenerics(sb, numFields);
            sb.append("> ProjectCross<I1, I2, Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">> projectTuple" + numFields + "(");
            sb.append(") {\n");

            // extract field types
            sb.append("\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);\n");

            // create new tuple type info
            sb.append("\t\t\tTupleTypeInfo<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">> tType = new TupleTypeInfo<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">>(fTypes);\n\n");

            // create and return new project operator
            sb.append("\t\t\treturn new ProjectCross<I1, I2, Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(
                    ">>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);\n");

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

        // method begin
        sb.append("\n");

        // method comment
        sb.append("\t\t/**\n");
        sb.append("\t\t * Chooses a projectTupleX according to the length of\n");
        sb.append(
                "\t\t * {@link org.apache.flink.api.java.operators.ProjectOperator.Projection#fieldIndexes} \n");
        sb.append("\t\t * \n");
        sb.append("\t\t * @return The projected DataSet.\n");
        sb.append("\t\t * \n");
        sb.append("\t\t * @see org.apache.flink.api.java.operators.ProjectOperator.Projection\n");
        sb.append("\t\t */\n");

        // method signature
        sb.append("\t\t@SuppressWarnings(\"unchecked\")\n");
        sb.append("\t\tpublic <OUT extends Tuple> ProjectOperator<T, OUT> projectTupleX() {\n");
        sb.append("\t\t\tProjectOperator<T, OUT> projOperator;\n\n");
        sb.append("\t\t\tswitch (fieldIndexes.length) {\n");
        for (int numFields = FIRST; numFields <= LAST; numFields++) {
            sb.append(
                    "\t\t\tcase "
                            + numFields
                            + ":"
                            + " projOperator = (ProjectOperator<T, OUT>) projectTuple"
                            + numFields
                            + "(); break;\n");
        }
        sb.append(
                "\t\t\tdefault: throw new IllegalStateException(\"Excessive arity in tuple.\");\n");
        sb.append("\t\t\t}\n\n");
        sb.append("\t\t\treturn projOperator;\n");

        // method end
        sb.append("\t\t}\n");

        for (int numFields = FIRST; numFields <= LAST; numFields++) {

            // method begin
            sb.append("\n");

            // method comment
            sb.append("\t\t/**\n");
            sb.append(
                    "\t\t * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. \n");
            sb.append("\t\t * \n");
            sb.append("\t\t * @return The projected DataSet.\n");
            sb.append("\t\t * \n");
            sb.append("\t\t * @see Tuple\n");
            sb.append("\t\t * @see DataSet\n");
            sb.append("\t\t */\n");

            // method signature
            sb.append("\t\tpublic <");
            appendTupleTypeGenerics(sb, numFields);
            sb.append("> ProjectOperator<T, Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">> projectTuple" + numFields + "(");
            sb.append(") {\n");

            // extract field types
            sb.append(
                    "\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());\n");

            // create new tuple type info
            sb.append("\t\t\tTupleTypeInfo<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">> tType = new TupleTypeInfo<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">>(fTypes);\n\n");

            // create and return new project operator
            sb.append("\t\t\treturn new ProjectOperator<T, Tuple" + numFields + "<");
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

        // method begin
        sb.append("\n");

        // method comment
        sb.append("\t\t/**\n");
        sb.append("\t\t * Chooses a projectTupleX according to the length of\n");
        sb.append(
                "\t\t * {@link org.apache.flink.api.java.operators.JoinOperator.JoinProjection#fieldIndexes}\n");
        sb.append("\t\t * \n");
        sb.append("\t\t * @return The projected DataSet.\n");
        sb.append("\t\t * \n");
        sb.append("\t\t * @see org.apache.flink.api.java.operators.JoinOperator.ProjectJoin\n");
        sb.append("\t\t */\n");

        // method signature
        sb.append("\t\t@SuppressWarnings(\"unchecked\")\n");
        sb.append("\t\tpublic <OUT extends Tuple> ProjectJoin<I1, I2, OUT> projectTupleX() {\n");
        sb.append("\t\t\tProjectJoin<I1, I2, OUT> projectJoin = null;\n\n");
        sb.append("\t\t\tswitch (fieldIndexes.length) {\n");
        for (int numFields = FIRST; numFields <= LAST; numFields++) {
            sb.append(
                    "\t\t\tcase "
                            + numFields
                            + ":"
                            + " projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple"
                            + numFields
                            + "(); break;\n");
        }
        sb.append(
                "\t\t\tdefault: throw new IllegalStateException(\"Excessive arity in tuple.\");\n");
        sb.append("\t\t\t}\n\n");
        sb.append("\t\t\treturn projectJoin;\n");

        // method end
        sb.append("\t\t}\n");

        for (int numFields = FIRST; numFields <= LAST; numFields++) {

            // method begin
            sb.append("\n");

            // method comment
            sb.append("\t\t/**\n");
            sb.append(
                    "\t\t * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. \n");
            sb.append("\t\t * Requires the classes of the fields of the resulting tuples. \n");
            sb.append("\t\t * \n");
            sb.append("\t\t * @return The projected data set.\n");
            sb.append("\t\t * \n");
            sb.append("\t\t * @see Tuple\n");
            sb.append("\t\t * @see DataSet\n");
            sb.append("\t\t */\n");

            // method signature
            sb.append("\t\tpublic <");
            appendTupleTypeGenerics(sb, numFields);
            sb.append("> ProjectJoin<I1, I2, Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">> projectTuple" + numFields + "(");
            sb.append(") {\n");

            // extract field types
            sb.append("\t\t\tTypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);\n");

            // create new tuple type info
            sb.append("\t\t\tTupleTypeInfo<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">> tType = new TupleTypeInfo<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">>(fTypes);\n\n");

            // create and return new project operator
            sb.append("\t\t\treturn new ProjectJoin<I1, I2, Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(
                    ">>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);\n");

            // method end
            sb.append("\t\t}\n");
        }

        // insert code into file
        File dir = getPackage(root, JOIN_OPERATOR_PACKAGE);
        File projectOperatorClass = new File(dir, JOIN_OPERATOR_CLASSNAME + ".java");
        insertCodeIntoFile(sb.toString(), projectOperatorClass);
    }

    private static void modifyCsvReader(File root) throws IOException {
        // generate code
        StringBuilder sb = new StringBuilder(1000);
        for (int numFields = FIRST; numFields <= LAST; numFields++) {

            // method begin
            sb.append("\n");

            // java doc
            sb.append("\t/**\n");
            sb.append(
                            "\t * Specifies the types for the CSV fields. This method parses the CSV data to a ")
                    .append(numFields)
                    .append("-tuple\n");
            sb.append("\t * which has fields of the specified types.\n");
            sb.append(
                    "\t * This method is overloaded for each possible length of the tuples to support type safe\n");
            sb.append("\t * creation of data sets through CSV parsing.\n");
            sb.append("\t *\n");

            for (int pos = 0; pos < numFields; pos++) {
                sb.append("\t * @param type").append(pos);
                sb.append(" The type of CSV field ").append(pos).append(" and the type of field ");
                sb.append(pos).append(" in the returned tuple type.\n");
            }
            sb.append(
                    "\t * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.\n");
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
            sb.append(">> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(");
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
            sb.append(">> inputFormat = new TupleCsvInputFormat<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">>(path, types, this.includedMask);\n");

            // configure input format
            sb.append("\t\tconfigureInputFormat(inputFormat);\n");

            // return
            sb.append("\t\treturn new DataSource<Tuple" + numFields + "<");
            appendTupleTypeGenerics(sb, numFields);
            sb.append(">>(executionContext, inputFormat, types, Utils.getCallLocationName());\n");

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
                    + " */"
                    + "\n"
                    + "\n"
                    + "\n"
                    + "// --------------------------------------------------------------\n"
                    + "//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!\n"
                    + "//  GENERATED FROM "
                    + TupleGenerator.class.getName()
                    + ".\n"
                    + "// --------------------------------------------------------------\n\n\n";
}
