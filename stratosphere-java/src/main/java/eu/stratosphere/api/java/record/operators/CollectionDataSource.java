/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.record.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.java.record.io.CollectionInputFormat;
import eu.stratosphere.types.Record;

/**
 * Operator for input nodes which reads data from collection or iterator.
 * Use this operator if you want to pass data from the application submitting the
 * Stratosphere job to the cluster.
 * There are two main ways to use the CollectionDataSource:
 * * Using a @link {@link SerializableIterator}
 * 
 * <pre>
 * CollectionDataSource source = new CollectionDataSource(new SerializableIteratorTest(), &quot;IterSource&quot;);
 * </pre>
 * 
 * * Using a Collection of Java Objects.
 * 
 * <pre>
 * CollectionDataSource source2 = new CollectionDataSource(new List&lt;String&gt;(), &quot;Collection source&quot;);
 * </pre>
 * 
 * Note that you can as many elements as you want to the constructor:
 * 
 * <pre>
 * CollectionDataSource(&quot;Varargs String source&quot;, &quot;some&quot;, &quot;strings&quot;, &quot;that&quot;, &quot;get&quot;, &quot;distributed&quot;);
 * </pre>
 * 
 * The only limitation is that the elements need to have the same type.
 */
public class CollectionDataSource extends GenericDataSourceBase<Record, GenericInputFormat<Record>> {

	private static String DEFAULT_NAME = "<Unnamed Collection Data Source>";

	/**
	 * Creates a new instance for the given input using the given input format.
	 * 
	 * @param f
	 *        The {@link CollectionInputFormat} implementation used to read the data.
	 * @param data
	 *        The input data. It should be a collection, an array or a serializable iterator.
	 * @param name
	 *        The given name for the Pact, used in plans, logs and progress messages.
	 */
	public CollectionDataSource(CollectionInputFormat f, String name, Object... data) {
		super(f, OperatorInfoHelper.source(), name);
		Collection<Object> tmp = new ArrayList<Object>();
		for (Object o : data) {
			tmp.add(o);
		}
		checkFormat(tmp);
		f.setData(tmp);
	}

	public CollectionDataSource(CollectionInputFormat f, String name, Object[][] data) {
		super(f, OperatorInfoHelper.source(), name);
		Collection<Object> tmp = new ArrayList<Object>();
		for (Object o : data) {
			tmp.add(o);
		}
		checkFormat(tmp);
		f.setData(tmp);
	}

	public CollectionDataSource(CollectionInputFormat f, Collection<?> data, String name) {
		super(f, OperatorInfoHelper.source(), name);
		checkFormat(data);
		f.setData(data);
	}

	public <T extends Iterator<?>, Serializable> CollectionDataSource(CollectionInputFormat f, T data, String name) {
		super(f, OperatorInfoHelper.source(), name);
		f.setIter(data);
	}

	/**
	 * Creates a new instance for the given input using the given input format. The contract has the default name.
	 * The input types will be checked. If the input types don't agree, an exception will occur.
	 * 
	 * @param args
	 *        The input data. It should be a collection, an array or a serializable iterator.
	 * @param name
	 *        The given name for the Pact, used in plans, logs and progress messages.
	 */
	public CollectionDataSource(String name, Object... args) {
		this(new CollectionInputFormat(), name, args);
	}

	public CollectionDataSource(String name, Object[][] args) {
		this(new CollectionInputFormat(), name, args);
	}

	public CollectionDataSource(Collection<?> args, String name) {
		this(new CollectionInputFormat(), args, name);
	}

	public <T extends Iterator<?>, Serializable> CollectionDataSource(T args, String name) {
		this(new CollectionInputFormat(), args, name);
	}

	// --------------------------------------------------------------------------------------------
	/**
	 * for scala compatible, scala-to-java type conversion always has an object wrapper
	 */
	public CollectionDataSource(Object... args) {
		this(new CollectionInputFormat(), args);
	}

	@SuppressWarnings("unchecked")
	public CollectionDataSource(CollectionInputFormat f, Object... data) {
		super(f, OperatorInfoHelper.source(), DEFAULT_NAME);
		if (data.length == 1 && data[0] instanceof Iterator) {
			f.setIter((Iterator<Object>) data[0]);
		}
		else if (data.length == 1 && data[0] instanceof Collection) {
			checkFormat((Collection<Object>) data[0]);
			f.setData((Collection<Object>) data[0]);
		}else {
			Collection<Object> tmp = new ArrayList<Object>();
			for (Object o : data) {
				tmp.add(o);
			}
			checkFormat(tmp);
			f.setData(tmp);
		}
	}

	// --------------------------------------------------------------------------------------------
	/*
	 * check whether the input field has the same type
	 */
	private <T> void checkFormat(Collection<T> c) {
		Class<?> type = null;
		List<Class<?>> typeList = new ArrayList<Class<?>>();
		Iterator<T> it = c.iterator();
		while (it.hasNext()) {
			Object o = it.next();

			// check the input types for 1-dimension
			if (type != null && !type.equals(o.getClass())) {
				throw new RuntimeException("elements of input list should have the same type");
			}
			else {
				type = o.getClass();
			}

			// check the input types for 2-dimension array
			if (typeList.size() == 0 && o.getClass().isArray()) {
				for (Object s : (Object[]) o) {
					typeList.add(s.getClass());
				}
			}
			else if (o.getClass().isArray()) {
				int index = 0;
				if (((Object[]) o).length != typeList.size()) {
					throw new RuntimeException("elements of input list should have the same size");
				}
				for (Object s : (Object[]) o) {
					if (!s.getClass().equals(typeList.get(index++))) {
						throw new RuntimeException("elements of input list should have the same type");
					}
				}
			}

			// check the input types for 2-dimension collection
			if (typeList.size() == 0 && o instanceof Collection) {
				@SuppressWarnings("unchecked")
				Iterator<Object> tmpIt = ((Collection<Object>) o).iterator();
				while (tmpIt.hasNext()) {
					Object s = tmpIt.next();
					typeList.add(s.getClass());
				}
			}
			else if (o instanceof Collection) {
				int index = 0;
				@SuppressWarnings("unchecked")
				Iterator<Object> tmpIt = ((Collection<Object>) o).iterator();
				while (tmpIt.hasNext()) {
					Object s = tmpIt.next();
					if (!s.getClass().equals(typeList.get(index++))) {
						throw new RuntimeException("elements of input list should have the same type");
					}
				}

				if (index != typeList.size()) {
					throw new RuntimeException("elements of input list should have the same size");
				}
			}
		}
	}

}
