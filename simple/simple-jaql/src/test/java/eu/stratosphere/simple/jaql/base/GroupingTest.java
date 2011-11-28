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
		final SopremoPlan actualPlan = this.parseScript("$employees = read 'employees.json';\n" +
			"$result = group $employees into count($);\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("employees.json");
		final Grouping selection = new Grouping().
			withResultProjection(new MethodCall("count", new InputSelection(0))).
			withInputs(input);
		final Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testGrouping2() {
		final SopremoPlan actualPlan = this.parseScript("$employees = read 'employees.json';\n" +
			"$result = group $employees by $.dept into {\n" +
			"	$.dept,\n" +
			"	total: sum($[*].income)\n" +
			"};\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("employees.json");
		final Grouping selection = new Grouping().
			withInputs(input).
			withGroupingKey(JsonUtil.createPath("0", "dept")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("0", "dept")),
				new ObjectCreation.FieldAssignment("total",
					new MethodCall("sum", JsonUtil.createPath("0", "[*]", "income")))));
		final Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testGrouping3() {
		final SopremoPlan actualPlan = this.parseScript("$employees = read 'employees.json';\n" +
			"$result = group $employee in $employees by $employee.dept into {\n" +
			"	$employee.dept, \n" +
			"	total: sum($employee[*].income) \n" +
			"};\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("employees.json");
		final Grouping selection = new Grouping().
			withInputs(input).
			withGroupingKey(JsonUtil.createPath("0", "dept")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("0", "dept")),
				new ObjectCreation.FieldAssignment("total",
					new MethodCall("sum", JsonUtil.createPath("0", "[*]", "income")))));
		final Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testCoGrouping1() {
		final SopremoPlan actualPlan = this.parseScript("$employees = read 'employees.json';\n" +
			"$depts = read 'departments.json';\n" +
			"$result = group $es in $employees by $es.dept,\n" +
			"	$ds in $depts by $ds.did into {\n" +
			"	dept: $ds.did,\n" +
			"	deptName: $ds[0].name,\n" +
			"	emps: $es[*].id,\n" +
			"	numEmps: count($es) \n" +
			"};\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source employees = new Source("employees.json");
		final Source depts = new Source("departments.json");
		final Grouping selection = new Grouping().
			withInputs(employees, depts).
			withGroupingKey(0, JsonUtil.createPath("0", "dept")).
			withGroupingKey(1, JsonUtil.createPath("1", "did")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("1", "did")),
				new ObjectCreation.FieldAssignment("deptName", JsonUtil.createPath("1", "[0]", "name")),
				new ObjectCreation.FieldAssignment("emps", JsonUtil.createPath("0", "[*]", "id")),
				new ObjectCreation.FieldAssignment("numEmps",
					new MethodCall("count", JsonUtil.createPath("0")))));
		final Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testCoGrouping2() {
		final SopremoPlan actualPlan = this.parseScript("$employees = read 'employees.json';\n" +
			"$depts = read 'departments.json';\n" +
			"$result = group $employees by $employees.dept,\n" +
			"	$depts by $depts.did into {\n" +
			"	dept: $depts.did,\n" +
			"	deptName: $depts[0].name,\n" +
			"	emps: $employees[*].id,\n" +
			"	numEmps: count($employees) \n" +
			"};\n" +
			"write $result to 'output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source employees = new Source("employees.json");
		final Source depts = new Source("departments.json");
		final Grouping selection = new Grouping().
			withInputs(employees, depts).
			withGroupingKey(0, JsonUtil.createPath("0", "dept")).
			withGroupingKey(1, JsonUtil.createPath("1", "did")).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("dept", JsonUtil.createPath("1", "did")),
				new ObjectCreation.FieldAssignment("deptName", JsonUtil.createPath("1", "[0]", "name")),
				new ObjectCreation.FieldAssignment("emps", JsonUtil.createPath("0", "[*]", "id")),
				new ObjectCreation.FieldAssignment("numEmps",
					new MethodCall("count", JsonUtil.createPath("0")))));
		final Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
