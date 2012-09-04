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
package eu.stratosphere.util;

/**
 * @author Arvid Heise
 */
public class ArrayUtil {
	/**
	 * Swaps the elements at the specified positions in the specified array.
	 * (If the specified positions are equal, invoking this method leaves
	 * the array unchanged.)
	 * 
	 * @param array
	 *        The array in which to swap elements.
	 * @param i
	 *        the index of one element to be swapped.
	 * @param j
	 *        the index of the other element to be swapped.
	 * @throws ArrayIndexOutOfBoundsException
	 *         if either <tt>i</tt> or <tt>j</tt> is out of range (i &lt; 0 || i &gt;= array.length || j &lt; 0 || j
	 *         &gt;= array.length).
	 */
	public static <T> void swap(T[] array, int i, int j) {
		T temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}

	/**
	 * Swaps the elements at the specified positions in the specified array.
	 * (If the specified positions are equal, invoking this method leaves
	 * the array unchanged.)
	 * 
	 * @param array
	 *        The array in which to swap elements.
	 * @param i
	 *        the index of one element to be swapped.
	 * @param j
	 *        the index of the other element to be swapped.
	 * @throws ArrayIndexOutOfBoundsException
	 *         if either <tt>i</tt> or <tt>j</tt> is out of range (i &lt; 0 || i &gt;= array.length || j &lt; 0 || j
	 *         &gt;= array.length).
	 */
	public static void swap(int[] array, int i, int j) {
		int temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}
}
