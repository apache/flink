package eu.stratosphere.sopremo.pact;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BigIntegerNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.DecimalNode;
import eu.stratosphere.sopremo.jsondatamodel.DoubleNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.LongNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.util.reflect.BoundTypeUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

public class JsonNodeComparator implements Comparator<JsonNode> {
	public final static JsonNodeComparator INSTANCE = new JsonNodeComparator();

	@SuppressWarnings("rawtypes")
	private final Map<Class<?>, Comparator> nodeComparators = new IdentityHashMap<Class<?>, Comparator>();

	public JsonNodeComparator() {
		for (final Class<?> subComparator : this.getClass().getDeclaredClasses())
			try {
				this.nodeComparators.put(BoundTypeUtil.getBindingOfSuperclass(subComparator, Comparator.class)
					.getParameters()[0].getType(), (Comparator<?>) subComparator.newInstance());
			} catch (Exception e) {
				e.printStackTrace();
			}

		this.nodeComparators.put(ArrayNode.class, ArrayNodeComparator.INSTANCE);

		final Comparator<ObjectNode> cannotCompare = new Comparator<ObjectNode>() {
			@Override
			public int compare(final ObjectNode o1, final ObjectNode o2) {
				throw new EvaluationException("Cannot compare two objects");
			}
		};
		/*
		 * this.nodeComparators.put(POJONode.class, cannotCompare);
		 * this.nodeComparators.put(BinaryNode.class, cannotCompare);
		 * this.nodeComparators.put(MissingNode.class, cannotCompare);
		 */
	}

	@Override
	public int compare(final JsonNode value1, final JsonNode value2) {
		final Class<? extends JsonNode> class1 = value1.getClass();
		final Class<? extends JsonNode> class2 = value2.getClass();
		if (class1 != class2)
			return class1.getSimpleName().compareTo(class2.getSimpleName());
		return this.compareStrict(value1, value2, class1);
	}

	@SuppressWarnings("unchecked")
	public int compareStrict(final JsonNode value1, final JsonNode value2, final Class<? extends JsonNode> clazz) {
		return this.nodeComparators.get(clazz).compare(value1, value2);
	}

	public final static class ArrayNodeComparator implements Comparator<JsonNode> {
		public static final Comparator INSTANCE = new ArrayNodeComparator();

		@Override
		public int compare(final JsonNode value1, final JsonNode value2) {
			final int size1 = ((ArrayNode) value1).size(), size2 = ((ArrayNode) value2).size();
			if (size1 != size2)
				return size1 - size2;
			for (int index = 0, size = size1; index < size; index++) {
				final int comparisonResult = JsonNodeComparator.INSTANCE.compare(((ArrayNode) value1).get(index),
					((ArrayNode) value2).get(index));
				if (comparisonResult != 0)
					return comparisonResult;
			}
			return 0;
		}
	}

	public final static class BigIntegerNodeComparator implements Comparator<BigIntegerNode> {
		@Override
		public int compare(final BigIntegerNode value1, final BigIntegerNode value2) {
			return value1.getBigIntegerValue().compareTo(value2.getBigIntegerValue());
		}
	}

	public final static class BooleanNodeComparator implements Comparator<BooleanNode> {
		@Override
		public int compare(final BooleanNode value1, final BooleanNode value2) {
			return value1.getBooleanValue() == value2.getBooleanValue() ? 0 : value1.getBooleanValue() ? 1 : -1;
		}
	}

	public final static class DecimalNodeComparator implements Comparator<DecimalNode> {
		@Override
		public int compare(final DecimalNode value1, final DecimalNode value2) {
			return value1.getDecimalValue().compareTo(value2.getDecimalValue());
		}
	}

	public final static class DoubleNodeComparator implements Comparator<DoubleNode> {
		@Override
		public int compare(final DoubleNode value1, final DoubleNode value2) {
			return Double.compare(value1.getDoubleValue(), value2.getDoubleValue());
		}
	}

	public final static class IntNodeComparator implements Comparator<IntNode> {
		@Override
		public int compare(final IntNode value1, final IntNode value2) {
			return value1.getIntValue() - value2.getIntValue();
		}
	}

	public final static class LongNodeComparator implements Comparator<LongNode> {
		@Override
		public int compare(final LongNode value1, final LongNode value2) {
			return Long.signum(value1.getLongValue() - value2.getLongValue());
		}
	}

	public final static class NullNodeComparator implements Comparator<NullNode> {
		@Override
		public int compare(final NullNode value1, final NullNode value2) {
			return 0;
		}
	}

	public final static class ObjectNodeComparator implements Comparator<ObjectNode> {
		@Override
		public int compare(final ObjectNode value1, final ObjectNode value2) {
			final Iterator<Entry<String, JsonNode>> fields1 = value1.getFields();
			final Iterator<Entry<String, JsonNode>> fields2 = value2.getFields();

			while (fields1.hasNext() && fields2.hasNext()) {
				final Entry<String, JsonNode> field1 = fields1.next();
				final Entry<String, JsonNode> field2 = fields2.next();

				final int keyComparison = field1.getKey().compareTo(field2.getKey());
				if (keyComparison != 0)
					return keyComparison;
				final int valueComparison = JsonNodeComparator.INSTANCE.compare(field1.getValue(), field2.getValue());
				if (valueComparison != 0)
					return valueComparison;
			}

			return fields1.hasNext() ? -1 : fields2.hasNext() ? 1 : 0;
		}
	}

	public final static class TextNodeComparator implements Comparator<TextNode> {
		@Override
		public int compare(final TextNode value1, final TextNode value2) {
			return value1.getTextValue().compareTo(value2.getTextValue());
		}
	}
}