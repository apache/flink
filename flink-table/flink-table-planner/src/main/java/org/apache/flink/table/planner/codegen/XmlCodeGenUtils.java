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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.functions.SqlXmlUtils;
import org.apache.flink.table.runtime.functions.XmlToVariantParser;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;

/**
 * Code generation of {@code PARSE_XML} and {@code TRY_PARSE_XML}.
 *
 * <p>The calls on the same input share the read document, so that each document is read once per
 * record. For example, {@code SELECT PARSE_XML(xml)['a'], PARSE_XML(xml, TRUE), TRY_PARSE_XML(xml)
 * FROM t} generates code similar to:
 *
 * <pre>{@code
 * // members
 * private final XmlToVariantParser xmlParser$0 = new XmlToVariantParser();
 * SqlXmlUtils.ReadXml xmlDocument$1;
 * BinaryStringData xmlInput$2;
 *
 * private SqlXmlUtils.ReadXml readXml$3(BinaryStringData in) {
 *   if (in != xmlInput$2) {
 *     xmlInput$2 = in;
 *     xmlDocument$1 = SqlXmlUtils.read(xmlParser$0, in);
 *   }
 *   return xmlDocument$1;
 * }
 *
 * // calls
 * result$4 = SqlXmlUtils.parseXml(readXml$3(field$0), false);
 * result$5 = SqlXmlUtils.parseXml(readXml$3(field$0), true);
 * result$6 = SqlXmlUtils.tryParseXml(readXml$3(field$0), false);
 * }</pre>
 *
 * <p>Whichever call runs first reads the document. A call can be skipped, e.g. in a branch of a
 * {@code CASE} that isn't taken, so no call can read it upfront for the others.
 */
public final class XmlCodeGenUtils {

    private XmlCodeGenUtils() {}

    public static GeneratedExpression generateParseXml(
            CodeGeneratorContext ctx, LogicalType returnType, Seq<GeneratedExpression> operands) {
        return generateCall(ctx, returnType, operands, "parseXml");
    }

    public static GeneratedExpression generateTryParseXml(
            CodeGeneratorContext ctx, LogicalType returnType, Seq<GeneratedExpression> operands) {
        return generateCall(ctx, returnType, operands, "tryParseXml");
    }

    /**
     * A {@code NULL} xml returns {@code NULL}. A {@code NULL} {@code force_array} is treated as
     * {@code false}, like the second argument of {@code PARSE_JSON}.
     */
    private static GeneratedExpression generateCall(
            CodeGeneratorContext ctx,
            LogicalType returnType,
            Seq<GeneratedExpression> operands,
            String method) {
        final String readXml = readSharedXml(ctx, operands.head());
        final String forceArrayCode;
        final String forceArray;
        if (operands.size() == 1) {
            forceArrayCode = "";
            forceArray = "false";
        } else {
            final GeneratedExpression operand = operands.apply(1);
            forceArrayCode = operand.code();
            forceArray = "(!" + operand.nullTerm() + " && " + operand.resultTerm() + ")";
        }
        final String call =
                String.format(
                        "%s.%s(%s, %s)",
                        SqlXmlUtils.class.getCanonicalName(), method, readXml, forceArray);
        // Only the xml is checked for NULL.
        return GenerateUtils.generateCallWithStmtIfArgsNotNull(
                ctx,
                returnType,
                JavaScalaConversionUtil.toScala(Collections.singletonList(operands.head())),
                true,
                false,
                argTerms -> new Tuple2<>(forceArrayCode, call));
    }

    /** Returns a call that reads the document of the input, or returns it if it was read. */
    private static String readSharedXml(CodeGeneratorContext ctx, GeneratedExpression xml) {
        final String key = "readXml(" + xml.resultTerm() + ")";
        final Option<GeneratedExpression> shared =
                ctx.getReusableInputUnboxingExprs(key, Integer.MIN_VALUE);
        if (shared.isDefined()) {
            return shared.get().resultTerm();
        }

        final String parserType = XmlToVariantParser.class.getCanonicalName();
        final String documentType = SqlXmlUtils.ReadXml.class.getCanonicalName();
        final String inputType = CodeGenUtils.boxedTypeTermForType(xml.resultType());
        final String parserTerm = CodeGenUtils.newName(ctx, "xmlParser");
        final String documentTerm = CodeGenUtils.newName(ctx, "xmlDocument");
        final String inputTerm = CodeGenUtils.newName(ctx, "xmlInput");
        final String methodTerm = CodeGenUtils.newName(ctx, "readXml");

        ctx.addReusableMember(
                String.format(
                        "private final %s %s = new %s();", parserType, parserTerm, parserType));
        ctx.addReusableMember(String.format("%s %s;", documentType, documentTerm));
        ctx.addReusableMember(String.format("%s %s;", inputType, inputTerm));
        // The input is a new object for each record, so the document is read again when it changes.
        ctx.addReusableMember(
                String.format(
                        "private %s %s(%s in) {\n"
                                + "  if (in != %s) {\n"
                                + "    %s = in;\n"
                                + "    %s = %s.read(%s, in);\n"
                                + "  }\n"
                                + "  return %s;\n"
                                + "}\n",
                        documentType,
                        methodTerm,
                        inputType,
                        inputTerm,
                        inputTerm,
                        documentTerm,
                        SqlXmlUtils.class.getCanonicalName(),
                        parserTerm,
                        documentTerm));

        final String call = methodTerm + "(" + xml.resultTerm() + ")";
        ctx.addReusableInputUnboxingExprs(
                key,
                Integer.MIN_VALUE,
                new GeneratedExpression(call, "false", "", null, Option.empty()));
        return call;
    }
}
