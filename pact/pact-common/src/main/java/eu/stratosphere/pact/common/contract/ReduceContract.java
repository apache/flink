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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;


/**
 * MapContract represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see ReduceStub
 */
public class ReduceContract extends SingleInputContract<ReduceStub>
{	
	private static final String DEFAULT_NAME = "<Unnamed Reducer>";	// the default name for contracts
	
	/**
	 * The ordering for the order inside a reduce group.
	 */
	private Ordering secondaryOrder;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClass The class of the key's type.
	 * @param keyColumn The position of the key in the input records.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key> keyClass, int keyColumn) {
		this(c, keyClass, keyColumn, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation
	 * and a default name. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClasses The classes of the data types of the key fields.
	 * @param keyColumns The positions of the key fields in the input records.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key>[] keyClasses, int[] keyColumns) {
		this(c, keyClasses, keyColumns, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClass The class of the key's type.
	 * @param keyColumn The position of the key in the input records.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key> keyClass,  int keyColumn, String name) {
		this(c, asArray(keyClass), new int[] {keyColumn}, name);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation 
	 * and the given name. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClasses The classes of the data types of the key fields.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key>[] keyClasses, int[] keyColumns, String name) {
		super(c, keyClasses, keyColumns, name);
	}

	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClass The class of the key's type.
	 * @param keyColumn The position of the key in the input records.
	 * @param input The contract to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key> keyClass, int keyColumn, Contract input) {
		this(c, keyClass, keyColumn, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClass The class of the key's type.
	 * @param keyColumn The position of the key in the input records.
	 * @param inputs The contracts to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key> keyClass, int keyColumn, List<Contract> inputs) {
		this(c, keyClass, keyColumn, inputs, DEFAULT_NAME);
	}

	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contract as its input. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClasses The classes of the data types of the key fields.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param input The contract to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key>[] keyClasses, int[] keyColumns, Contract input) {
		this(c, keyClasses, keyColumns, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contracts as its input. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClasses The classes of the data types of the key fields.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param inputs The contracts to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key>[] keyClasses, int[] keyColumns, List<Contract> inputs) {
		this(c, keyClasses, keyColumns, inputs, DEFAULT_NAME);
	}

	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClass The class of the key's type.
	 * @param keyColumn The position of the key in the input records.
	 * @param input The contract to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key> keyClass, int keyColumn, Contract input, String name) {
		this(c, keyClass, keyColumn, name);
		setInput(input);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClass The class of the key's type.
	 * @param keyColumn The position of the key in the input records.
	 * @param inputs The contracts to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key> keyClass, int keyColumn, List<Contract> inputs, String name) {
		this(c, keyClass, keyColumn, name);
		setInputs(inputs);
	}

	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contract as its input. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClasses The classes of the data types of the key fields.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param input The contract to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key>[] keyClasses, int[] keyColumns, Contract input, String name) {
		this(c, keyClasses, keyColumns, name);
		setInput(input);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contracts as its input. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyClasses The classes of the data types of the key fields.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param inputs The contracts to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub> c, Class<? extends Key>[] keyClasses, int[] keyColumns, List<Contract> inputs, String name) {
		this(c, keyClasses, keyColumns, name);
		setInputs(inputs);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	private ReduceContract(Builder builder) {
		super(builder.udf, builder.keyClasses, builder.keyColumns, builder.name);
		setInputs(builder.inputs);
		this.secondaryOrder = builder.secondaryOrder;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the order of the elements within a reduce group.
	 * 
	 * @param order The order for the elements in a reduce group.
	 */
	public void setSecondaryOrder(Ordering order) {
		this.secondaryOrder = order;
	}
	
	/**
	 * Gets the secondary order, i.e. the order of elements within a reduce group. If no such order has been
	 * set, this method returns null.
	 * 
	 * @return The secondary order.
	 */
	public Ordering getSecondaryOrder() {
		return this.secondaryOrder;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns true if the ReduceContract is annotated with a Combinable annotation.
	 * The annotation indicates that the contract's {@link ReduceStub} implements the 
	 * {@link ReduceStub#combine(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)}
	 * method.
	 * 
	 * @return True, if the ReduceContract is combinable, false otherwise.
	 */
	public boolean isCombinable()
	{
		return getUserCodeClass().getAnnotation(Combinable.class) != null;
	}
	
	/**
	 * This annotation marks reduce stubs as eligible for the usage of a combiner.
	 * 
	 * The following code excerpt shows how to make a simple reduce stub combinable (assuming here that
	 * the reducer function and combiner function do the same):
	 * 
	 * <code>
	 * \@Combinable
	 * public static class CountWords extends ReduceStub&lt;PactString&gt;
	 * {
	 *     private final PactInteger theInteger = new PactInteger();
	 * 
	 *     \@Override
	 *     public void reduce(PactString key, Iterator&lt;PactRecord&gt; records, Collector out) throws Exception
	 *     {
	 *         PactRecord element = null;
	 *         int sum = 0;
	 *         while (records.hasNext()) {
	 *             element = records.next();
	 *             element.getField(1, this.theInteger);
	 *             // we could have equivalently used PactInteger i = record.getField(1, PactInteger.class);
	 *          
	 *             sum += this.theInteger.getValue();
	 *         }
	 *      
	 *         element.setField(1, this.theInteger);
	 *         out.collect(element);
	 *     }
	 *     
	 *     public void combine(PactString key, Iterator&lt;PactRecord&gt; records, Collector out) throws Exception
	 *     {
	 *         this.reduce(key, records, out);
	 *     }
	 * }
	 * </code>
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Combinable {};
	
	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 * 
	 * @author Aljoscha Krettek
	 */
	public static class Builder {
		/**
		 * The required parameters.
		 */
		private final Class<? extends ReduceStub> udf;
		private final Class<? extends Key>[] keyClasses;
		private final int[] keyColumns;
		
		/**
		 * The optional parameters.
		 */
		private Ordering secondaryOrder = null;
		private List<Contract> inputs;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link CoGroupStub} implementation.
		 * 
		 * @param udf The {@link CoGroupStub} implementation for this CoGroup InputContract.
		 * @param keyClass The class of the key data type.
		 * @param keyColumn The position of the key.
		 */
		public Builder(Class<? extends ReduceStub> udf, Class<? extends Key> keyClass, int keyColumn) {
			this(udf, asArray(keyClass), new int[] {keyColumn});
		}
		
		/**
		 * Creates a Builder with the provided {@link CoGroupStub} implementation.
		 * 
		 * @param udf The {@link CoGroupStub} implementation for this CoGroup InputContract.
		 * @param keyClass The classes of the key data types.
		 * @param keyColumn The positions of the keys.
		 */
		public Builder(Class<? extends ReduceStub> udf, Class<? extends Key>[] keyClasses, int[] keyColumns) {
			this.udf = udf;
			this.keyClasses = keyClasses;
			this.keyColumns = keyColumns;
			this.inputs = new LinkedList<Contract>();
		}

		/**
		 * Sets the order of the elements within a group.
		 * 
		 * @param order The order for the elements in a group.
		 */
		public Builder secondaryOrder(Ordering order) {
			this.secondaryOrder = order;
			return this;
		}
		
		/**
		 * Sets the input.
		 * 
		 * @param input
		 */
		public Builder input(Contract input) {
			this.inputs.clear();
			this.inputs.add(input);
			return this;
		}
		
		/**
		 * Sets the inputs.
		 * 
		 * @param input
		 */
		public Builder inputs(List<Contract> inputs) {
			this.inputs = inputs;
			return this;
		}
		
		/**
		 * Sets the name of this contract.
		 * 
		 * @param name
		 */
		public Builder name(String name) {
			this.name = name;
			return this;
		}
		
		/**
		 * Creates and returns a ReduceContract from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public ReduceContract build() {
			return new ReduceContract(this);
		}
	}
}
