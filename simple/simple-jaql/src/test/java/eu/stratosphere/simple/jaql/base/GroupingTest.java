/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

/**
 * @author Arvid Heise
 */
public class GroupingTest extends SimpleTest {

	@Test
	public void testGrouping1() {
		SopremoPlan actualPlan = parseScript("$employees = read 'employees.json';" +
			"$result = group $employees into count($);" +
			"write $result to 'output.json'; ");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source input = new Source("employees.json");
		Grouping selection = new Grouping().
			withResultProjection(new MethodCall("count", new InputSelection(0))).
			withInputs(input);
		Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testGrouping2() {
		SopremoPlan actualPlan = parseScript("$employees = read 'employees.json';" +
			"$result = group $employees by $.dept into {" +
			"	$.dept," +
			"	total: sum($[*].income)" +
			"};" +
			"write $result to 'output.json'; ");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source input = new Source("employees.json");
		Grouping selection = new Grouping().
			withInputs(input).
			withGroupingKey(JsonUtil.createPath("0", "dept")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("0", "dept")),
				new ObjectCreation.FieldAssignment("total",
					new MethodCall("sum", JsonUtil.createPath("0", "*", "income")))));
		Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}
	
	@Test
	public void testGrouping3() {
		SopremoPlan actualPlan = parseScript("$employees = read 'employees.json';" +
			"$result = group $employee in $employees by $employee.dept into {" +
			"	$employee.dept, " +
			"	total: sum($employee[*].income) " +
			"};" +
			"write $result to 'output.json'; ");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source input = new Source("employees.json");
		Grouping selection = new Grouping().
			withInputs(input).
			withGroupingKey(JsonUtil.createPath("0", "dept")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("0", "dept")),
				new ObjectCreation.FieldAssignment("total",
					new MethodCall("sum", JsonUtil.createPath("0", "[*]", "income")))));
		Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}
	
	@Test
	public void testCoGrouping1() {
		SopremoPlan actualPlan = parseScript("$employees = read 'employees.json';" +
				"$depts = read 'departments.json';" +
				"$result = group $es in $employees by $es.dept," +
				"	$ds in $depts by $ds.did into {" +
				"	dept: $ds.did," +
				"	deptName: $ds[0].name," +
				"	emps: $es[*].id," +
				"	numEmps: count($es) " +
				"};" +
				"write $result to 'output.json'; ");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source employees = new Source("employees.json");
		Source depts = new Source("departments.json");
		Grouping selection = new Grouping().
			withInputs(employees, depts).
			withGroupingKey(0, JsonUtil.createPath("0", "dept")).
			withGroupingKey(1, JsonUtil.createPath("0", "did")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("1", "did")),
				new ObjectCreation.FieldAssignment("deptName", JsonUtil.createPath("1", "[0]", "name")),
				new ObjectCreation.FieldAssignment("emps", JsonUtil.createPath("0", "[*]", "id")),
				new ObjectCreation.FieldAssignment("numEmps",
					new MethodCall("count", JsonUtil.createPath("0")))));
		Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}
	
	@Test
	public void testCoGrouping2() {
		SopremoPlan actualPlan = parseScript("$employees = read 'employees.json';" +
				"$depts = read 'departments.json';" +
				"$result = group $employees by $employees.dept," +
				"	$depts by $depts.did into {" +
				"	dept: $depts.did," +
				"	deptName: $depts[0].name," +
				"	emps: $employees[*].id," +
				"	numEmps: count($employees) " +
				"};" +
				"write $result to 'output.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source employees = new Source("employees.json");
		Source depts = new Source("departments.json");
		Grouping selection = new Grouping().
			withInputs(employees, depts).
			withGroupingKey(0, JsonUtil.createPath("0", "dept")).
			withGroupingKey(1, JsonUtil.createPath("0", "did")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("1", "did")),
				new ObjectCreation.FieldAssignment("deptName", JsonUtil.createPath("1", "[0]", "name")),
				new ObjectCreation.FieldAssignment("emps", JsonUtil.createPath("0", "[*]", "id")),
				new ObjectCreation.FieldAssignment("numEmps",
					new MethodCall("count", JsonUtil.createPath("0")))));
		Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
