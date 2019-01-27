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

package org.apache.flink.sql.parser.util;

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.fail;

/**
 * An abstract base class for implementing tests against {@link SqlValidator}.
 *
 * <p>A derived class can refine this test in two ways. First, it can add <code>
 * testXxx()</code> methods, to test more functionality.
 */
public class SqlValidatorTestCase {

	//~ Static fields/initializers ---------------------------------------------

	private static final Pattern LINE_COL_PATTERN =
		Pattern.compile("At line ([0-9]+), column ([0-9]+)");

	private static final Pattern LINE_COL_TWICE_PATTERN =
		Pattern.compile(
			"(?s)From line ([0-9]+), column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)");

	/**
	 * Checks whether an exception matches the expected pattern. If <code>
	 * sap</code> contains an error location, checks this too.
	 *
	 * @param ex                 Exception thrown
	 * @param expectedMsgPattern Expected pattern
	 * @param sap                Query and (optional) position in query
	 */
	public static void checkEx(
		Throwable ex,
		String expectedMsgPattern,
		SqlParserUtil.StringAndPos sap) {
		if (null == ex) {
			if (expectedMsgPattern == null) {
				// No error expected, and no error happened.
				return;
			} else {
				throw new AssertionError("Expected query to throw exception, "
					+ "but it did not; query [" + sap.sql
					+ "]; expected [" + expectedMsgPattern + "]");
			}
		}
		Throwable actualException = ex;
		String actualMessage = actualException.getMessage();
		int actualLine = -1;
		int actualColumn = -1;
		int actualEndLine = 100;
		int actualEndColumn = 99;

		// Search for an CalciteContextException somewhere in the stack.
		CalciteContextException ece = null;
		for (Throwable x = ex; x != null; x = x.getCause()) {
			if (x instanceof CalciteContextException) {
				ece = (CalciteContextException) x;
				break;
			}
			if (x.getCause() == x) {
				break;
			}
		}

		// Search for a SqlParseException -- with its position set -- somewhere
		// in the stack.
		SqlParseException spe = null;
		for (Throwable x = ex; x != null; x = x.getCause()) {
			if ((x instanceof SqlParseException)
				&& (((SqlParseException) x).getPos() != null)) {
				spe = (SqlParseException) x;
				break;
			}
			if (x.getCause() == x) {
				break;
			}
		}

		if (ece != null) {
			actualLine = ece.getPosLine();
			actualColumn = ece.getPosColumn();
			actualEndLine = ece.getEndPosLine();
			actualEndColumn = ece.getEndPosColumn();
			if (ece.getCause() != null) {
				actualException = ece.getCause();
				actualMessage = actualException.getMessage();
			}
		} else if (spe != null) {
			actualLine = spe.getPos().getLineNum();
			actualColumn = spe.getPos().getColumnNum();
			actualEndLine = spe.getPos().getEndLineNum();
			actualEndColumn = spe.getPos().getEndColumnNum();
			if (spe.getCause() != null) {
				actualException = spe.getCause();
				actualMessage = actualException.getMessage();
			}
		} else {
			final String message = ex.getMessage();
			if (message != null) {
				Matcher matcher = LINE_COL_TWICE_PATTERN.matcher(message);
				if (matcher.matches()) {
					actualLine = Integer.parseInt(matcher.group(1));
					actualColumn = Integer.parseInt(matcher.group(2));
					actualEndLine = Integer.parseInt(matcher.group(3));
					actualEndColumn = Integer.parseInt(matcher.group(4));
					actualMessage = matcher.group(5);
				} else {
					matcher = LINE_COL_PATTERN.matcher(message);
					if (matcher.matches()) {
						actualLine = Integer.parseInt(matcher.group(1));
						actualColumn = Integer.parseInt(matcher.group(2));
					}
				}
			}
		}

		if (null == expectedMsgPattern) {
			actualException.printStackTrace();
			fail("Validator threw unexpected exception"
				+ "; query [" + sap.sql
				+ "]; exception [" + actualMessage
				+ "]; class [" + actualException.getClass()
				+ "]; pos [line " + actualLine
				+ " col " + actualColumn
				+ " thru line " + actualLine
				+ " col " + actualColumn + "]");
		}

		String sqlWithCarets;
		if (actualColumn <= 0
			|| actualLine <= 0
			|| actualEndColumn <= 0
			|| actualEndLine <= 0) {
			if (sap.pos != null) {
				AssertionError e =
					new AssertionError("Expected error to have position,"
						+ " but actual error did not: "
						+ " actual pos [line " + actualLine
						+ " col " + actualColumn
						+ " thru line " + actualEndLine + " col "
						+ actualEndColumn + "]");
				e.initCause(actualException);
				throw e;
			}
			sqlWithCarets = sap.sql;
		} else {
			sqlWithCarets =
				SqlParserUtil.addCarets(
					sap.sql,
					actualLine,
					actualColumn,
					actualEndLine,
					actualEndColumn + 1);
			if (sap.pos == null) {
				throw new AssertionError("Actual error had a position, but expected "
					+ "error did not. Add error position carets to sql:\n"
					+ sqlWithCarets);
			}
		}

		if (actualMessage != null) {
			actualMessage = Util.toLinux(actualMessage);
		}

		if (actualMessage == null
			|| !actualMessage.matches(expectedMsgPattern)) {
			actualException.printStackTrace();
			final String actualJavaRegexp =
				(actualMessage == null)
					? "null"
					: TestUtil.quoteForJava(
					TestUtil.quotePattern(actualMessage));
			fail("Validator threw different "
				+ "exception than expected; query [" + sap.sql
				+ "];\n"
				+ " expected pattern [" + expectedMsgPattern
				+ "];\n"
				+ " actual [" + actualMessage
				+ "];\n"
				+ " actual as java regexp [" + actualJavaRegexp
				+ "]; pos [" + actualLine
				+ " col " + actualColumn
				+ " thru line " + actualEndLine
				+ " col " + actualEndColumn
				+ "]; sql [" + sqlWithCarets + "]");
		} else if (sap.pos != null
			&& (actualLine != sap.pos.getLineNum()
			|| actualColumn != sap.pos.getColumnNum()
			|| actualEndLine != sap.pos.getEndLineNum()
			|| actualEndColumn != sap.pos.getEndColumnNum())) {
			fail("Validator threw expected "
				+ "exception [" + actualMessage
				+ "];\nbut at pos [line " + actualLine
				+ " col " + actualColumn
				+ " thru line " + actualEndLine
				+ " col " + actualEndColumn
				+ "];\nsql [" + sqlWithCarets + "]");
		}
	}
}
