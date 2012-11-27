/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.meteor;

import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.junit.Ignore;

import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * @author Arvid Heise
 */
@Ignore
public class MeteorTest {
	/**
	 * @param expectedPlan
	 * @param actualPlan
	 */
	protected static void assertPlanEquals(final SopremoPlan expectedPlan, final SopremoPlan actualPlan) {
		final List<Operator<?>> unmatchingOperators = actualPlan.getUnmatchingOperators(expectedPlan);
		if (!unmatchingOperators.isEmpty())
			if (unmatchingOperators.get(0).getClass() == unmatchingOperators.get(1).getClass())
				Assert.failNotEquals("operators are different", "\n" + unmatchingOperators.get(1), "\n"
					+ unmatchingOperators.get(0));
			else
				Assert.failNotEquals("plans are different", expectedPlan, actualPlan);
	}

	public SopremoPlan parseScript(final String script) {
//		printBeamerSlide(script);
		SopremoPlan plan = null;
		try {
			plan = new QueryParser().tryParse(script);
			// System.out.println(new QueryParser().toJavaString(script));
		} catch (final QueryParserException e) {
			final AssertionFailedError error = new AssertionFailedError(String.format("could not parse script: %s", e.getMessage()));
			error.initCause(e);
			throw error;
		}

		Assert.assertNotNull("could not parse script", plan);

		// System.out.println(plan);
		return plan;
	}

	protected void printBeamerSlide(final String script) {
		StackTraceElement[] stackTrace = new Throwable().getStackTrace();
		System.out.println("\\begin{frame}[fragile]{" +
			stackTrace[2].getClassName().replaceAll(".*\\.(.*)Test", "$1") +
			"}");
		System.out.println("\\lstset{");
		System.out.println("\tkeywords=[5]{} % operators");
		System.out.println("\tkeywords=[6]{} % properties");
		System.out.println("\tkeywords=[7]{} % functions");
		System.out.println("\tkeywords=[8]{" + findVars(script) + "}");
		System.out.println("}");
		System.out.println("\\begin{lstlisting}");
		System.out.println(script);
		System.out.println("\\end{lstlisting}");
		System.out.println("\\end{frame}");
		System.out.println();
	}

	protected String findVars(final String script) {
		TreeSet<String> vars = new TreeSet<String>();
		Matcher matcher = Pattern.compile("\\$\\w+").matcher(script);
		while(matcher.find())
			vars.add(matcher.group());
		String varString = vars.toString();
		return varString.substring(1, varString.length() - 1);
	}
}
