package eu.stratosphere.sopremo;

import java.awt.Image;
import java.beans.BeanDescriptor;
import java.beans.BeanInfo;
import java.beans.EventSetDescriptor;
import java.beans.FeatureDescriptor;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.beans.SimpleBeanInfo;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * Base class for all Sopremo operators. Every operator consumes and produces a specific number of {@link JsonStream}s.
 * The operator groups input json objects accordingly to its semantics and transforms the partitioned objects to one or
 * more outputs.<br>
 * Each Sopremo operator may be converted to a {@link PactModule} with the {@link #asPactModule(EvaluationContext)}
 * method.<br>
 * Implementations of an operator should either extend {@link ElementaryOperator} or {@link CompositeOperator}.
 * 
 * @author Arvid Heise
 */
@InputCardinality(min = 1, max = 1)
public abstract class Operator<Self extends Operator<Self>> implements SerializableSopremoType, JsonStream, Cloneable,
		BeanInfo {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7808932536291658512L;

	private transient List<Operator<?>.Output> inputs = new ArrayList<Operator<?>.Output>();

	private String name;

	private transient List<Output> outputs = new ArrayList<Output>();

	private int minInputs, maxInputs;

	/**
	 * Initializes the Operator with the number of outputs set to 1.
	 */
	public Operator() {
		this(1);
	}

	/**
	 * Initializes the Operator with the given number of outputs.
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 */
	public Operator(int numberOfOutputs) {
		this.setNumberOfOutputs(numberOfOutputs);
		InputCardinality inputs = ReflectUtil.getAnnotation(this.getClass(), InputCardinality.class);
		this.setNumberOfInputs(inputs.min(), inputs.max());
		this.name = getClass().getSimpleName();
	}

	/**
	 * Converts this operator to a {@link PactModule} using the provided {@link EvaluationContext}.
	 * 
	 * @param context
	 *        the context in which the evaluation should be conducted
	 * @return the {@link PactModule} representing this operator
	 */
	public abstract PactModule asPactModule(EvaluationContext context);

	@Override
	public Operator<Self> clone() {
		try {
			@SuppressWarnings("unchecked")
			Operator<Self> clone = (Operator<Self>) super.clone();
			clone.inputs = new ArrayList<Operator<?>.Output>(this.inputs);
			clone.outputs = new ArrayList<Operator<Self>.Output>(this.outputs);
			return clone;
		} catch (final CloneNotSupportedException e) {
			// cannot happen
			return null;
		}
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Operator<?> other = (Operator<?>) obj;
		return this.name.equals(other.name);
	}

	// /**
	// * Initializes the Operator with the given number of outputs and the given input {@link JsonStream}s. A JsonStream
	// * is either the output of another operator or the operator itself.
	// *
	// * @param numberOfOutputs
	// * the number of outputs
	// * @param inputs
	// * the input JsonStreams produces by other operators
	// */
	// Operator(final int numberOfOutputs, final JsonStream... inputs) {
	// this(1, Arrays.asList(inputs));
	// }
	//
	// /**
	// * Initializes the Operator with the given number of outputs, and the given input {@link JsonStream}s. A
	// JsonStream
	// * is either the output of another operator or the operator itself.
	// *
	// * @param numberOfOutputs
	// * the number of outputs
	// * @param inputs
	// * the input JsonStreams produces by other operators
	// */
	// Operator(final int numberOfOutputs, final List<? extends JsonStream> inputs) {
	// if (inputs == null)
	// throw new NullPointerException();
	// if (numberOfOutputs < 0)
	// throw new IllegalArgumentException("numberOfOutputs < 0");
	//
	// for (final JsonStream input : inputs)
	// this.inputs.add(input == null ? null : input.getSource());
	// this.name = this.getClass().getSimpleName();
	// this.setNumberOfOutputs(numberOfOutputs);
	// }
	//
	// /**
	// * Initializes the Operator with the given input {@link JsonStream}s. A JsonStream is
	// * either the output of another operator or the operator itself. The number of outputs is set to 1.
	// *
	// * @param inputs
	// * the input JsonStreams produces by other operators
	// */
	// Operator(final JsonStream... inputs) {
	// this(1, inputs);
	// }
	//
	// /**
	// * Initializes the Operator with the given input {@link JsonStream}s. A JsonStream is
	// * either the output of another operator or the operator itself. The number of outputs is set to 1.
	// *
	// * @param inputs
	// * the input JsonStreams produces by other operators
	// */
	// Operator(final List<? extends JsonStream> inputs) {
	// this(1, inputs);
	// }

	@Override
	public BeanInfo[] getAdditionalBeanInfo() {
		return this.getBeanInfo().getAdditionalBeanInfo();
	}

	@Override
	public BeanDescriptor getBeanDescriptor() {
		return this.getBeanInfo().getBeanDescriptor();
	}

	private static Map<Class<?>, Info> beanInfos = new IdentityHashMap<Class<?>, Operator.Info>();

	private BeanInfo getBeanInfo() {
		Info beanInfo = beanInfos.get(getClass());
		if (beanInfo == null)
			beanInfos.put(getClass(), beanInfo = new Info(this.getClass()));
		return beanInfo;
	}

	@Override
	public int getDefaultEventIndex() {
		return this.getBeanInfo().getDefaultEventIndex();
	}

	@Override
	public int getDefaultPropertyIndex() {
		return this.getBeanInfo().getDefaultPropertyIndex();
	}

	@Override
	public EventSetDescriptor[] getEventSetDescriptors() {
		return this.getBeanInfo().getEventSetDescriptors();
	}

	@Override
	public Image getIcon(int iconKind) {
		return this.getBeanInfo().getIcon(iconKind);
	}

	/**
	 * Returns the output of an operator producing the {@link JsonStream} that is the input to this operator at the
	 * given position.
	 * 
	 * @param index
	 *        the index of the output
	 * @return the output that produces the input of this operator at the given position
	 */
	public Operator<?>.Output getInput(final int index) {
		return this.inputs.get(index);
	}

	/**
	 * Returns a list of operators producing the {@link JsonStream}s that are the inputs to this operator.<br>
	 * If multiple outputs of an operator are used as inputs for this operator, the operator appears several times.
	 * 
	 * @return a list of operators that produce the input of this operator
	 */
	public List<Operator<?>> getInputOperators() {
		return new AbstractList<Operator<?>>() {

			@Override
			public Operator<?> get(final int index) {
				return Operator.this.inputs.get(index) == null ? null : Operator.this.inputs.get(index).getOperator();
			}

			@Override
			public int indexOf(final Object o) {
				final ListIterator<Operator<?>.Output> e = Operator.this.inputs.listIterator();
				while (e.hasNext())
					if (o == e.next())
						return e.previousIndex();
				return -1;
			}

			@Override
			public int size() {
				return Operator.this.inputs.size();
			}
		};
	}

	/**
	 * Returns a list of outputs of operators producing the {@link JsonStream}s that are the inputs to this operator.<br>
	 * If an output is used multiple times as inputs for this operator, the output appears several times (for example in
	 * a self-join).
	 * 
	 * @return a list of outputs that produce the input of this operator
	 */
	public List<Operator<?>.Output> getInputs() {
		return new ArrayList<Operator<?>.Output>(this.inputs);
	}

	public int getMaxInputs() {
		return this.maxInputs;
	}

	@Override
	public MethodDescriptor[] getMethodDescriptors() {
		return this.getBeanInfo().getMethodDescriptors();
	}

	public int getMinInputs() {
		return this.minInputs;
	}

	/**
	 * The name of this operator, which is the class name by default.
	 * 
	 * @return the name of this operator.
	 * @see #setName(String)
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Returns the output at the specified index.
	 * 
	 * @param index
	 *        the index to lookup
	 * @return the output at the given position
	 */
	public Output getOutput(final int index) {
		return this.outputs.get(index);
	}

	/**
	 * Returns all outputs of this operator.
	 * 
	 * @return all outputs of this operator
	 */
	public List<Output> getOutputs() {
		return new ArrayList<Operator<Self>.Output>(this.outputs);
	}

	@Override
	public PropertyDescriptor[] getPropertyDescriptors() {
		return this.getBeanInfo().getPropertyDescriptors();
	}

	/**
	 * Returns the first output of this operator.
	 */
	@Override
	public Output getSource() {
		return this.getOutput(0);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.name.hashCode();
		return result;
	}

	/**
	 * Replaces the input at the given location with the given {@link JsonStream}s.
	 * 
	 * @param index
	 *        the index of the input
	 * @param input
	 *        the new input
	 */
	public void setInput(final int index, final JsonStream input) {
		if (index >= this.maxInputs)
			throw new IndexOutOfBoundsException();

		CollectionUtil.ensureSize(this.inputs, index + 1);
		this.inputs.set(index, input == null ? null : input.getSource());
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 */
	public void setInputs(final JsonStream... inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");
		if (this.minInputs > inputs.length || inputs.length > this.maxInputs)
			throw new IndexOutOfBoundsException();

		this.inputs.clear();
		for (final JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 */
	public void setInputs(final List<? extends JsonStream> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");
		if (this.minInputs > inputs.size() || inputs.size() > this.maxInputs)
			throw new IndexOutOfBoundsException();

		this.inputs.clear();
		for (final JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 * @return this
	 */
	public Self withInputs(final List<? extends JsonStream> inputs) {
		setInputs(inputs);
		return self();
	}

	@SuppressWarnings("unchecked")
	protected final Self self() {
		return (Self) this;
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 * @return this
	 */
	public Self withInputs(final JsonStream... inputs) {
		setInputs(inputs);
		return self();
	}

	protected void setNumberOfInputs(int min, int max) {
		if (min > max)
			throw new IllegalArgumentException();
		if (min < 0 || max < 0)
			throw new IllegalArgumentException();
		this.minInputs = min;
		this.maxInputs = max;
		CollectionUtil.ensureSize(this.inputs, this.minInputs);
	}

	protected void setNumberOfInputs(int num) {
		this.setNumberOfInputs(num, num);
	}

	/**
	 * Sets the name of this operator.
	 * 
	 * @param name
	 *        the new name of this operator
	 */
	public void setName(final String name) {
		if (name == null)
			throw new NullPointerException("name must not be null");

		this.name = name;
	}

	/**
	 * Sets the number of outputs of this operator retaining all old outputs if possible (increased number of outputs).
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 */
	protected final void setNumberOfOutputs(final int numberOfOutputs) {
		if (numberOfOutputs > outputs.size())
			outputs.subList(numberOfOutputs, outputs.size()).clear();
		else
			CollectionUtil.ensureSize(outputs, numberOfOutputs);
	}

	public SopremoModule toElementaryOperators() {
		SopremoModule module = new SopremoModule(this.getName(), this.getInputs().size(), this.getOutputs().size());
		Operator<Self> clone = this.clone();
		for (int index = 0; index < this.getInputs().size(); index++)
			clone.setInput(index, module.getInput(index));
		for (int index = 0; index < this.getOutputs().size(); index++)
			module.getOutput(index).setInput(index, clone.getOutput(index));
		return module;
	}

	@Override
	public String toString() {
		return this.getName();
	}

	public void validate() throws IllegalStateException {
		for (int index = 0; index < this.inputs.size(); index++)
			if (this.inputs.get(index) == null)
				throw new IllegalStateException("unconnected input " + index);
	}

	public static class Info extends SimpleBeanInfo {
		public static final String NAME_PREPOSITION = "name.preposition";

		public static final String NAME_ADJECTIVE = "name.adjective";

		public static final String NAME_VERB = "name.verb";

		public static final String NAME_NOUNS = "name.nouns";

		public static final String INPUT = "flag.input";

		private BeanDescriptor classDescriptor;

		private PropertyDescriptor[] properties;

		@SuppressWarnings("rawtypes")
		public Info(Class<? extends Operator> clazz) {
			this.classDescriptor = new BeanDescriptor(clazz);
			this.setNames(this.classDescriptor, clazz.getAnnotation(Name.class));

			this.findProperties(clazz);
		}

		@SuppressWarnings("rawtypes")
		private void findProperties(Class<? extends Operator> clazz) {
			List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
			try {
				for (PropertyDescriptor descriptor : Introspector.getBeanInfo(clazz, 0).getPropertyDescriptors()) {
					Method writeMethod = descriptor instanceof IndexedPropertyDescriptor ?
						((IndexedPropertyDescriptor) descriptor).getIndexedWriteMethod() :
						descriptor.getWriteMethod();
					Property propertyDescription;
					if (writeMethod != null
						&& (propertyDescription = writeMethod.getAnnotation(Property.class)) != null) {
						descriptor.setHidden(propertyDescription.hidden());
						properties.add(descriptor);

						descriptor.setValue(INPUT, propertyDescription.input());
						this.setNames(descriptor, writeMethod.getAnnotation(Name.class));
					}
				}
			} catch (IntrospectionException e) {
				e.printStackTrace();
			}
			this.properties = properties.toArray(new PropertyDescriptor[properties.size()]);
			// for (Method m : clazz.getMethods()) {
			// // skip non-public or static methods
			// if (((m.getModifiers() & Modifier.STATIC) != 0) || ((m.getModifiers() & Modifier.PUBLIC) == 0))
			// continue;
			//
			// switch(m.getParameterTypes().length) {
			// case 0:
			//
			// }
			// }
		}

		@Override
		public BeanDescriptor getBeanDescriptor() {
			return this.classDescriptor;
		}

		@Override
		public PropertyDescriptor[] getPropertyDescriptors() {
			return this.properties;
		}

		private void setNames(FeatureDescriptor description, Name annotation) {
			if (annotation != null) {
				description.setValue(NAME_NOUNS, annotation.noun());
				description.setValue(NAME_VERB, annotation.verb());
				description.setValue(NAME_ADJECTIVE, annotation.adjective());
				description.setValue(NAME_PREPOSITION, annotation.preposition());
			} else {
				String[] empty = new String[0];
				description.setValue(NAME_NOUNS, empty);
				description.setValue(NAME_VERB, empty);
				description.setValue(NAME_ADJECTIVE, empty);
				description.setValue(NAME_PREPOSITION, empty);
			}
		}
	}

	/**
	 * Represents one output of this {@link Operator}. The output should be connected to another Operator to create a
	 * directed acyclic graph of Operators.
	 * 
	 * @author Arvid Heise
	 */
	public class Output implements JsonStream, Cloneable {
		private final int index;

		private Output(final int index) {
			this.index = index;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			@SuppressWarnings("unchecked")
			final Operator<Self>.Output other = (Operator<Self>.Output) obj;
			return this.index == other.index && this.getOperator() == other.getOperator();
		}

		/**
		 * Returns the index of this output in the list of outputs of the associated operator.
		 * 
		 * @return the index of this output
		 */
		public int getIndex() {
			return this.index;
		}

		/**
		 * Returns the associated operator.
		 * 
		 * @return the associated operator
		 */
		public Operator<Self> getOperator() {
			return Operator.this;
		}

		@Override
		public Output getSource() {
			return this;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.index;
			result = prime * result + this.getOperator().hashCode();
			return result;
		}

		@Override
		public String toString() {
			return String.format("%s@%d", this.getOperator(), this.index);
		}
	}

}