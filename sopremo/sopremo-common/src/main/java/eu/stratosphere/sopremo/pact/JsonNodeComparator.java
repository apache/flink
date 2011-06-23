package eu.stratosphere.sopremo.pact;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BigIntegerNode;
import org.codehaus.jackson.node.BinaryNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.MissingNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.util.reflect.BoundTypeUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

public class JsonNodeComparator implements Comparator<JsonNode> {
	public final static JsonNodeComparator INSTANCE = new JsonNodeComparator();

	@SuppressWarnings("rawtypes")
	private Map<Class<?>, Comparator> nodeComparators = new IdentityHashMap<Class<?>, Comparator>();

	public JsonNodeComparator() {
		for (Class<?> subComparator : this.getClass().getDeclaredClasses())
			this.nodeComparators.put(BoundTypeUtil.getBindingOfSuperclass(subComparator, Comparator.class).getParameters()[0].getType(),
				(Comparator<?>) ReflectUtil.getStaticValue(subComparator, "INSTANCE"));

		this.nodeComparators.put(ArrayNode.class, ArrayNodeComparator.INSTANCE);
		this.nodeComparators.put(CompactArrayNode.class, ArrayNodeComparator.INSTANCE);
		this.nodeComparators.put(StreamArrayNode.class, ArrayNodeComparator.INSTANCE);

		Comparator<ObjectNode> cannotCompare = new Comparator<ObjectNode>() {
			@Override
			public int compare(ObjectNode o1, ObjectNode o2) {
				throw new EvaluationException("Cannot compare two objects");
			}
		};
		this.nodeComparators.put(ObjectNode.class, cannotCompare);
		this.nodeComparators.put(POJONode.class, cannotCompare);
		this.nodeComparators.put(BinaryNode.class, cannotCompare);
		this.nodeComparators.put(MissingNode.class, cannotCompare);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(JsonNode value1, JsonNode value2) {
		return nodeComparators.get(value1.getClass()).compare(value1, value2);
	}

	public final static class BigIntegerNodeComparator implements Comparator<BigIntegerNode> {
		public final static JsonNodeComparator.BigIntegerNodeComparator INSTANCE = new BigIntegerNodeComparator();

		@Override
		public int compare(BigIntegerNode value1, BigIntegerNode value2) {
			return value1.getBigIntegerValue().compareTo(value2.getBigIntegerValue());
		}
	}

	public final static class DecimalNodeComparator implements Comparator<DecimalNode> {
		public final static JsonNodeComparator.DecimalNodeComparator INSTANCE = new DecimalNodeComparator();

		@Override
		public int compare(DecimalNode value1, DecimalNode value2) {
			return value1.getDecimalValue().compareTo(value2.getDecimalValue());
		}
	}

	public final static class DoubleNodeComparator implements Comparator<DoubleNode> {
		public final static JsonNodeComparator.DoubleNodeComparator INSTANCE = new DoubleNodeComparator();

		@Override
		public int compare(DoubleNode value1, DoubleNode value2) {
			return Double.compare(value1.getDoubleValue(), value2.getDoubleValue());
		}
	}

	public final static class LongNodeComparator implements Comparator<LongNode> {
		public final static JsonNodeComparator.LongNodeComparator INSTANCE = new LongNodeComparator();

		@Override
		public int compare(LongNode value1, LongNode value2) {
			return Long.signum(value1.getLongValue() - value2.getLongValue());
		}
	}

	public final static class IntNodeComparator implements Comparator<IntNode> {
		public final static JsonNodeComparator.IntNodeComparator INSTANCE = new IntNodeComparator();

		@Override
		public int compare(IntNode value1, IntNode value2) {
			return value1.getIntValue() - value2.getIntValue();
		}
	}

	public final static class NullNodeComparator implements Comparator<NullNode> {
		public final static JsonNodeComparator.NullNodeComparator INSTANCE = new NullNodeComparator();

		@Override
		public int compare(NullNode value1, NullNode value2) {
			return 0;
		}
	}

	public final static class BooleanNodeComparator implements Comparator<BooleanNode> {
		public final static JsonNodeComparator.BooleanNodeComparator INSTANCE = new BooleanNodeComparator();

		@Override
		public int compare(BooleanNode value1, BooleanNode value2) {
			return value1.getBooleanValue() == value2.getBooleanValue() ? 0 : value1.getBooleanValue() ? 1 : -1;
		}
	}

	public final static class TextNodeComparator implements Comparator<TextNode> {
		public final static JsonNodeComparator.TextNodeComparator INSTANCE = new TextNodeComparator();

		@Override
		public int compare(TextNode value1, TextNode value2) {
			return value1.getTextValue().compareTo(value2.getTextValue());
		}
	}

	public final static class ArrayNodeComparator implements Comparator<JsonNode> {
		public final static JsonNodeComparator.ArrayNodeComparator INSTANCE = new ArrayNodeComparator();

		@Override
		public int compare(JsonNode value1, JsonNode value2) {
			if (value1.size() != value2.size())
				return value1.size() - value2.size();
			for (int index = 0, size = value1.size(); index < size; index++) {
				int comparisonResult = JsonNodeComparator.INSTANCE.compare(value1.get(index), value2.get(index));
				if (comparisonResult != 0)
					return comparisonResult;
			}
			return 0;
		}
	}
}