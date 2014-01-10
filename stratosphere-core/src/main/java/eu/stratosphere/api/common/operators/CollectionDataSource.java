package eu.stratosphere.api.common.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.io.CollectionInputFormat;
import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.api.common.operators.util.SerializableIterator;

/**
 * Operator for input nodes which reads data from collection or iterator.
 * 
 */
public class CollectionDataSource extends GenericDataSource<GenericInputFormat<?>> {

	private static String DEFAULT_NAME = "<Unnamed Collection Data Source>";
	

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new instance for the given input using the given input format.
	 * 
	 * @param f The {@link CollectionInputFormat} implementation used to read the data.
	 * @param data The input data. It should be a collection, an array or a {@link SerializableIterator}.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public CollectionDataSource(CollectionInputFormat f, Collection<Object> data, String name) {
		super(f, name);
		checkFormat(data);
		f.setData(data);
	}	
	
	/**
	 * Creates a new instance for the given input using the given input format. The contract has the default name.
	 * The input types will be checked. If the input types don't agree, an exception will occur.
	 * 
	 * @param f The {@link CollectionInputFormat} implementation used to read the data.
	 * @param data The input data. It should be a collection, an array or a {@link SerializableIterator}.
	 */
	@SuppressWarnings("unchecked")
	public CollectionDataSource(CollectionInputFormat f, Object[] data) {
		super(f, DEFAULT_NAME);
		if (data.length == 1 && data[0] instanceof SerializableIterator) {
			f.setIter((SerializableIterator<Object>)data[0]);
		}
		else if (data.length == 1 && data[0] instanceof Collection) {
			f.setData((Collection<Object>)data[0]);
		}
		else {
			Collection<Object> tmp = new ArrayList<Object>();
			for (Object o : data) {
				tmp.add(o);
			}
			checkFormat(tmp);
			f.setData(tmp);
		}
		
	}
	
	
	/**
	 * Creates a new instance for the given input using the given input format. The contract has the default name.
	 * The input types will be checked. If the input types don't agree, an exception will occur.
	 * 
	 * @param args The input data. It should be a collection, an array or a {@link SerializableIterator}.
	 */
	public CollectionDataSource(Object... args) {
		this(new CollectionInputFormat(), args);
	}
	
	public CollectionDataSource(Object[][] args) {
		this(new CollectionInputFormat(), args);
	}
	
	public CollectionDataSource(Collection<Object> args) {
		this(new CollectionInputFormat(), args, DEFAULT_NAME);
	}
	
	

	
	// --------------------------------------------------------------------------------------------
	/*
	 * check whether the input field has the same type
	 */
	private void checkFormat(Collection<Object> c) {
		String type = null;
		List<String> typeList = new ArrayList<String>();
		Iterator<Object> it = c.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			//check the input types for 1-dimension
			if (type != null && !type.equals(o.getClass().getName())) {
				throw new RuntimeException("elements of input list should have the same type");
			}
			else {
				type = o.getClass().getName();
			}
		
			//check the input types for 2-dimension array
			if (typeList.size() == 0 && o.getClass().isArray()) {
				for (Object s: (Object[])o) {
					typeList.add(s.getClass().getName());
				}
			}
			else if (o.getClass().isArray()) {
				int index = 0;
				if (((Object[])o).length != typeList.size()) {
					throw new RuntimeException("elements of input list should have the same size");
				}
				for (Object s:(Object[])o) 
					if (!s.getClass().getName().equals(typeList.get(index++))) {
						throw new RuntimeException("elements of input list should have the same type");
					}
			}
			
			//check the input types for 2-dimension collection
			if (typeList.size() == 0 && o instanceof Collection) {
				@SuppressWarnings("unchecked")
				Iterator<Object> tmpIt = ((Collection<Object>) o).iterator();
				while (tmpIt.hasNext())
				{
					Object s = tmpIt.next();
					typeList.add(s.getClass().getName());
				}
			}
			else if (o instanceof Collection) {
				int index = 0;
				@SuppressWarnings("unchecked")
				Iterator<Object> tmpIt = ((Collection<Object>) o).iterator();
				while (tmpIt.hasNext()) {
					Object s = tmpIt.next();
					if (!s.getClass().getName().equals(typeList.get(index++))) {
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
