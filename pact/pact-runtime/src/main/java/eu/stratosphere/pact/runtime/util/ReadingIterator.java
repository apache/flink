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

package eu.stratosphere.pact.runtime.util;

import java.io.IOException;

/**
 *
 * IMPORTANT NOTE: Dealing with mutable objects requires careful code, otherwise a reference which
 * is kept in two places is updated in one place, while the other place assumes it is kept constant.
 * Aggressive deep-copying is typically avoided for performance reasons. Therefore, the implementing 
 * mutable object iterators must follow a simple contract, which avoids situations of accidental
 * updating:
 * <p>
 * <h3>General Contract</h3>
 * <b>Part 1:</b> When implementing the mutable object iterator, make use of the objects that are given
 * to the <code>next(E target)</code> method. Avoid in all cases returning the same object again and again.
 * For example, when implementing an iterator that generates data, do not return the same object every time,
 * only with updated fields), but set the fields into the given object and return it instead.
 * The following is an example of how to do it:
 * <code><pre>
 * public class MyGenerator implements MutableObjectIterator<MyType> {
 * 
 *     private int i = 0;
 *     
 *     public MyType next(MyType target) {
 *         target.setValue(i++);
 *         return target;
 *     }
 * }
 * </pre></code>
 * <p>
 * In the example below, he object from the previous call is used, and the target object is buffered for the
 * next call:
 * <code><pre>
 * public class MyGenerator implements MutableObjectIterator<MyType> {
 * 
 *     private MyType last = new MyType();
 *     private int i = 0;
 *     
 *     public MyType next(MyType target) {
 *         MyType t = this.last;
 *         this.last = target;
 *         t.setValue(i++);
 *         return t;
 *     }
 * }
 * </pre></code>
 * <p>
 * In the following, an example is given how NOT to do it:
 * <code><pre>
 * public class MyGenerator implements MutableObjectIterator<MyType> {
 * 
 *     private final MyType type = new MyType()
 *     private int i = 0;
 *     
 *     public MyType next(MyType target) {
 *         this.type.setValue(i++);
 *         return this.type;
 *         // ignore the given target object
 *     }
 * }
 * </pre></code>
 * <p>
 * <b>Part 2:</b>
 * When the iterator in turn obtains its object from another mutable object iterator (such as for example an iterator
 * implementing a merge operation takes objects from iterators that serve the sorted initial runs), then 
 * the implementor must make sure that no object that is assumed to be constant has been given to the object source
 * iterator. That rule frequently results in a pattern where one or two extra objects are maintained, which are in
 * a turn handed to the object source and are replaced by the target from the <code>next(E target)</code> method. 
 *
 * @author Stephan Ewen
 */
public interface ReadingIterator<E>
{
	public E next(E target) throws IOException;
}
