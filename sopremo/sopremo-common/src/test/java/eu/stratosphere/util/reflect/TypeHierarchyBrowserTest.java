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
package eu.stratosphere.util.reflect;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.util.reflect.TypeHierarchyBrowser.Mode;

@RunWith(Parameterized.class)
public class TypeHierarchyBrowserTest {

	private static final Visitor<Class<?>> DEFAULT_VISITOR = new Visitor<Class<?>>() {
		@Override
		public boolean visited(Class<?> node, int distance) {
			return true;
		}
	};

	@Parameters
	public static List<Object[]> combinations() {
		final ArrayList<Object[]> cases = new ArrayList<Object[]>();
		cases.add(new Object[] { Object.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues() });
		// class only
		cases.add(new Object[] { A.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(Object.class, 1) });
		cases.add(new Object[] { B.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(A.class, 1).add(Object.class, 2) });
		cases.add(new Object[] { C.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(B.class, 1).add(A.class, 2).add(Object.class, 3) });
		// class with interfaces
		cases.add(new Object[] { AI.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(Object.class, 1).add(I.class, 1) });
		cases.add(new Object[] { BI.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(A.class, 1).add(I.class, 1).add(Object.class, 2) });
		cases.add(new Object[] { CI.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(B.class, 1).add(J.class, 1).add(A.class, 2).add(I.class, 2).add(Object.class, 3) });
		cases.add(new Object[] { DI.class, Mode.CLASS_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(CI.class, 1).add(I.class, 1).add(B.class, 2).add(J.class, 2).add(A.class, 3).
				add(I.class, 3).add(Object.class, 4) });
		// class with interfaces and filter
		cases.add(new Object[] { AI.class, Mode.CLASS_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(Object.class, 1) });
		cases.add(new Object[] { BI.class, Mode.CLASS_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(A.class, 1).add(Object.class, 2) });
		cases.add(new Object[] { CI.class, Mode.CLASS_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(B.class, 1).add(A.class, 2).add(Object.class, 3) });
		cases.add(new Object[] { DI.class, Mode.CLASS_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(CI.class, 1).add(B.class, 2).add(A.class, 3).add(Object.class, 4) });
		// class with interfaces and filter
		cases.add(new Object[] { AI.class, Mode.INTERFACE_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(I.class, 1) });
		cases.add(new Object[] { BI.class, Mode.INTERFACE_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(I.class, 1) });
		cases.add(new Object[] { CI.class, Mode.INTERFACE_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(J.class, 1).add(I.class, 2) });
		cases.add(new Object[] { DI.class, Mode.INTERFACE_ONLY, Integer.MAX_VALUE, null,
			new ExpectedValues().add(I.class, 1).add(J.class, 2).add(I.class, 3) });

		// unsorted ALL mode
		cases.add(new Object[] { AI.class, Mode.ALL, Integer.MAX_VALUE, null,
			new ExpectedValues().add(Object.class, 1).add(I.class, 1) });
		cases.add(new Object[] { BI.class, Mode.ALL, Integer.MAX_VALUE, null,
			new ExpectedValues().add(A.class, 1).add(I.class, 1).add(Object.class, 2) });
		cases.add(new Object[] { CI.class, Mode.ALL, Integer.MAX_VALUE, null,
			new ExpectedValues().add(B.class, 1).add(J.class, 1).add(A.class, 2).add(I.class, 2).add(Object.class, 3) });
		cases.add(new Object[] { DI.class, Mode.ALL, Integer.MAX_VALUE, null,
			new ExpectedValues().add(CI.class, 1).add(I.class, 1).add(B.class, 2).add(J.class, 2).add(A.class, 3).
				add(I.class, 3).add(Object.class, 4) });
		// interface first
		cases.add(new Object[] { AI.class, Mode.INTERFACE_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(I.class, 1).add(Object.class, 1) });
		cases.add(new Object[] { BI.class, Mode.INTERFACE_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(I.class, 1).add(A.class, 1).add(Object.class, 2) });
		cases.add(new Object[] { CI.class, Mode.INTERFACE_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(J.class, 1).add(B.class, 1).add(I.class, 2).add(A.class, 2).add(Object.class, 3) });
		cases.add(new Object[] { DI.class, Mode.INTERFACE_FIRST, Integer.MAX_VALUE, null,
			new ExpectedValues().add(I.class, 1).add(CI.class, 1).add(J.class, 2).add(B.class, 2).
				add(I.class, 3).add(A.class, 3).add(Object.class, 4) });

		// max depth
		cases.add(new Object[] { DI.class, Mode.ALL, 2, null,
			new ExpectedValues().add(CI.class, 1).add(I.class, 1).add(B.class, 2).add(J.class, 2) });
		cases.add(new Object[] { DI.class, Mode.ALL, 3, null,
			new ExpectedValues().add(CI.class, 1).add(I.class, 1).add(B.class, 2).add(J.class, 2).add(A.class, 3).
				add(I.class, 3) });

		// premature termination
		cases.add(new Object[] { DI.class, Mode.INTERFACE_FIRST, Integer.MAX_VALUE, new Visitor<Class<?>>() {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.reflect.Visitor#visited(java.lang.Object, int)
			 */
			@Override
			public boolean visited(Class<?> node, int distance) {
				return node != J.class;
			}
		}, new ExpectedValues().add(I.class, 1).add(CI.class, 1).add(J.class, 2) });

		return cases;
	}

	private static class ExpectedValues {
		private List<AbstractMap.SimpleEntry<Class<?>, Integer>> values =
			new ArrayList<AbstractMap.SimpleEntry<Class<?>, Integer>>();

		public ExpectedValues add(Class<?> clazz, int distance) {
			this.values.add(new AbstractMap.SimpleEntry<Class<?>, Integer>(clazz, distance));
			return this;
		}

		/**
		 * Returns the values.
		 * 
		 * @return the values
		 */
		public List<AbstractMap.SimpleEntry<Class<?>, Integer>> getValues() {
			return this.values;
		}
	}

	private Class<?> startClass;

	private Mode mode;

	private int maxDepth;

	private List<? extends Map.Entry<Class<?>, Integer>> expectedClasses;

	private Visitor<Class<?>> visitor;

	public TypeHierarchyBrowserTest(Class<?> startClass, Mode mode, int maxDepth, Visitor<Class<?>> visitor,
			ExpectedValues expectedClasses) {
		this.startClass = startClass;
		this.mode = mode;
		this.maxDepth = maxDepth;
		this.expectedClasses = expectedClasses.getValues();
		this.visitor = visitor != null ? visitor : DEFAULT_VISITOR;
	}

	@Test
	public void test() {
		final List<SimpleEntry<Class<?>, Integer>> actual = new ArrayList<SimpleEntry<Class<?>, Integer>>();
		TypeHierarchyBrowser.INSTANCE.visit(this.startClass, this.mode, new Visitor<Class<?>>() {
			@Override
			public boolean visited(Class<?> node, int distance) {
				actual.add(new SimpleEntry<Class<?>, Integer>(node, distance));
				return TypeHierarchyBrowserTest.this.visitor.visited(node, distance);
			}
		}, this.maxDepth);

		if (this.mode == Mode.ALL)
			Assert.assertEquals(String.format("Failed to browser %s (%s) to %d", this.startClass, this.mode,
				this.maxDepth),
				new HashSet<Map.Entry<Class<?>, Integer>>(this.expectedClasses),
				new HashSet<Map.Entry<Class<?>, Integer>>(actual));
		else
			Assert.assertEquals(String.format("Failed to browser %s (%s) to %d", this.startClass, this.mode,
				this.maxDepth), this.expectedClasses, actual);
	}

	private static class A {
	}

	private static class B extends A {
	}

	private static class C extends B {
	}

	private static interface I {
	}

	private static interface J extends I {
	}

	private static class AI implements I {
	}

	private static class BI extends A implements I {
	}

	private static class CI extends B implements J {
	}

	private static class DI extends CI implements I {
	}
}