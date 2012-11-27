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
package eu.stratosphere.pact.testing;

/**
 * Represents the external definition of {@link Object#equals(Object)}. <code>Equaler</code> is needed when the class of
 * objects does not have an appropriate {@link Object#equals(Object)} definition.
 * 
 * @author Arvid Heise
 */
public interface Equaler<T> {
	/**
	 * Returns true, if both objects are equal; false, otherwise.<br>
	 * This method has similar semantic to <code>object1.equals(object2)</code>.
	 * 
	 * @param object1
	 *        the first object
	 * @param object2
	 *        the second object
	 * @return true, if both objects are equal; false, otherwise.
	 */
	public boolean equal(T object1, T object2);

	public static Equaler<Object> JavaEquals = new JavaEquals(), SafeEquals = new SafeEquals();

	/**
	 * Wraps {@link Object#equals(Object)}.
	 * 
	 * @author Arvid Heise
	 */
	public static final class JavaEquals implements Equaler<Object> {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.testing.Equaler#equal(java.lang.Object, java.lang.Object)
		 */
		@Override
		public boolean equal(Object object1, Object object2) {
			return object1.equals(object2);
		}
	}

	/**
	 * Wraps {@link Object#equals(Object)} but honors possible <code>null</code> values.
	 * 
	 * @author Arvid Heise
	 */
	public static final class SafeEquals implements Equaler<Object> {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.testing.Equaler#equal(java.lang.Object, java.lang.Object)
		 */
		@Override
		public boolean equal(Object object1, Object object2) {
			return object1 == null ? object2 == null : object1.equals(object2);
		}
	}
}
