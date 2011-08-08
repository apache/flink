package eu.stratosphere.sopremo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BigIntegerNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

public class TypeCoercer {
	@SuppressWarnings("unchecked")
	private static final Class<? extends JsonNode>[] ARRAY_TYPES =
		(Class<? extends JsonNode>[]) new Class<?>[] { ArrayNode.class, CompactArrayNode.class, StreamArrayNode.class };

	/**
	 * The default instance.
	 */
	public static final TypeCoercer INSTANCE = new TypeCoercer();

	private final Map<Class<? extends JsonNode>, Map<Class<? extends JsonNode>, Coercer>> coercers = new IdentityHashMap<Class<? extends JsonNode>, Map<Class<? extends JsonNode>, Coercer>>();

	private static final Coercer NULL_COERCER = new Coercer() {
		@Override
		public JsonNode coerce(final JsonNode node) {
			return null;
		}
	};

	public TypeCoercer() {
		this.coercers.put(BooleanNode.class, this.getToBooleanCoercers());
		this.coercers.put(TextNode.class, this.getToStringCoercers());
		this.coercers.put(ArrayNode.class, this.getToArrayCoercers());
		this.coercers.put(ObjectNode.class, new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>());
		this.addNumericCoercers(this.coercers);
		this.addSelfCoercers();
	}

	private void addNumericCoercers(
			final Map<Class<? extends JsonNode>, Map<Class<? extends JsonNode>, Coercer>> coercers) {
		coercers.put(NumericNode.class, new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>());
		final Class<? extends NumericNode>[] NUMERIC_TYPES = (Class<? extends NumericNode>[]) new Class<?>[] {
			IntNode.class,
			DoubleNode.class, LongNode.class, DecimalNode.class, BigIntegerNode.class };

		// init number to number
		for (final Class<? extends NumericNode> numericType : NUMERIC_TYPES) {
			final IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer> typeCoercers = new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>();
			coercers.put(numericType, typeCoercers);
			typeCoercers.put(NumericNode.class, NumberCoercer.INSTANCE.getClassCoercers().get(numericType));
		}

		// boolean to number
		coercers.get(IntNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return IntNode.valueOf(node == BooleanNode.TRUE ? 1 : 0);
			}
		});
		coercers.get(DoubleNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return DoubleNode.valueOf(node == BooleanNode.TRUE ? 1 : 0);
			}
		});
		coercers.get(LongNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return LongNode.valueOf(node == BooleanNode.TRUE ? 1 : 0);
			}
		});
		coercers.get(DecimalNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return DecimalNode.valueOf(BigDecimal.valueOf(node == BooleanNode.TRUE ? 1 : 0));
			}
		});
		coercers.get(BigIntegerNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return BigIntegerNode.valueOf(BigInteger.valueOf(node == BooleanNode.TRUE ? 1 : 0));
			}
		});
		// default boolean to number conversion -> int
		coercers.get(NumericNode.class).put(BooleanNode.class,
			coercers.get(IntNode.class).get(BooleanNode.class));

		// string to number
		coercers.get(IntNode.class).put(TextNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				try {
					return IntNode.valueOf(Integer.parseInt(node.getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(DoubleNode.class).put(TextNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				try {
					return DoubleNode.valueOf(Double.parseDouble(node.getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(LongNode.class).put(TextNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				try {
					return LongNode.valueOf(Long.parseLong(node.getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(DecimalNode.class).put(TextNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				try {
					return DecimalNode.valueOf(new BigDecimal(node.getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(BigIntegerNode.class).put(TextNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				try {
					return BigIntegerNode.valueOf(new BigInteger(node.getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		// default boolean to number conversion -> decimal
		coercers.get(NumericNode.class).put(TextNode.class,
			coercers.get(DecimalNode.class).get(TextNode.class));
	}

	private void addSelfCoercers() {
		final Coercer selfCoercer = new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return node;
			}
		};
		for (final Entry<Class<? extends JsonNode>, Map<Class<? extends JsonNode>, Coercer>> toCoercers : this.coercers
			.entrySet())
			toCoercers.getValue().put(toCoercers.getKey(), selfCoercer);
	}

	public <T extends JsonNode> T coerce(final JsonNode node, final Class<T> targetType) {
		final T result = this.coerce(node, targetType, null);
		if (result == null)
			throw new CoercionException(String.format("Cannot coerce %s to %s", node, targetType));
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T extends JsonNode> T coerce(final JsonNode node, final Class<T> targetType, final T defaultValue) {
		final Map<Class<? extends JsonNode>, Coercer> toCoercer = this.coercers.get(targetType);
		Coercer fromCoercer = toCoercer.get(node.getClass());
		if (fromCoercer == null) {
			for (Class<?> superType = node.getClass(); superType != JsonNode.class.getSuperclass()
				&& fromCoercer == null; superType = superType
				.getSuperclass())
				fromCoercer = toCoercer.get(superType);
			if (fromCoercer == null)
				fromCoercer = NULL_COERCER;
			toCoercer.put(node.getClass(), fromCoercer);
		}

		T result = (T) fromCoercer.coerce(node);
		if (result == null)
			result = defaultValue;
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T extends JsonNode> T coerce(final JsonNode node, final T defaultValue) {
		return this.coerce(node, (Class<T>) defaultValue.getClass(), defaultValue);
	}

	private Map<Class<? extends JsonNode>, Coercer> getToArrayCoercers() {
		final Map<Class<? extends JsonNode>, Coercer> toArrayCoercers = new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>();
		toArrayCoercers.put(JsonNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				final ArrayNode arrayNode = new ArrayNode(null);
				arrayNode.add(node);
				return arrayNode;
			}
		});
		final Coercer containerToArray = new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				final ArrayNode arrayNode = new ArrayNode(null);
				final Iterator<JsonNode> iterator = node.iterator();
				while (iterator.hasNext())
					arrayNode.add(iterator.next());
				return arrayNode;
			}
		};
		for (final Class<? extends JsonNode> arrayType : ARRAY_TYPES)
			toArrayCoercers.put(arrayType, containerToArray);
		toArrayCoercers.put(ObjectNode.class, containerToArray);
		return toArrayCoercers;
	}

	private Map<Class<? extends JsonNode>, Coercer> getToBooleanCoercers() {
		final Map<Class<? extends JsonNode>, TypeCoercer.Coercer> toBooleanCoercers = new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>();
		toBooleanCoercers.put(NumericNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return BooleanNode.valueOf(node.getValueAsDouble() != 0);
			}
		});
		toBooleanCoercers.put(TextNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return BooleanNode.valueOf(node.getTextValue().length() > 0);
			}
		});
		final Coercer containerToBoolean = new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return BooleanNode.valueOf(node.size() > 0);
			}
		};
		for (final Class<? extends JsonNode> arrayType : ARRAY_TYPES)
			toBooleanCoercers.put(arrayType, containerToBoolean);
		toBooleanCoercers.put(ObjectNode.class, containerToBoolean);
		return toBooleanCoercers;
	}

	private Map<Class<? extends JsonNode>, Coercer> getToStringCoercers() {
		final Map<Class<? extends JsonNode>, TypeCoercer.Coercer> toStringCoercers = new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>();
		toStringCoercers.put(JsonNode.class, new Coercer() {
			@Override
			public JsonNode coerce(final JsonNode node) {
				return TextNode.valueOf(node.toString());
			}
		});
		return toStringCoercers;
	}

	public void setCoercer(final Class<? extends JsonNode> from, final Class<? extends JsonNode> to,
			final Coercer coercer) {
		Map<Class<? extends JsonNode>, Coercer> toCoercers = this.coercers.get(to);
		if (toCoercers == null)
			this.coercers.put(to, toCoercers = new IdentityHashMap<Class<? extends JsonNode>, TypeCoercer.Coercer>());
		toCoercers.put(from, coercer);
	}

	public static interface Coercer {
		public JsonNode coerce(JsonNode node);
	}
}
