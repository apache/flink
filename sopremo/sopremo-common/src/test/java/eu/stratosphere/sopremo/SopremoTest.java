package eu.stratosphere.sopremo;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.List;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.util.reflect.BoundTypeUtil;

@Ignore
public abstract class SopremoTest<T> {
	protected T first, second;

	protected T[] more;

	protected Class<T> type;

	protected T createDefaultInstance(final int index) {
		try {
			return this.type.newInstance();
		} catch (final Exception e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	protected void createInstances() {
		this.initInstances(this.createDefaultInstance(0), this.createDefaultInstance(1), this.createDefaultInstance(2));
	}

	@SuppressWarnings("unchecked")
	@Before
	public void initInstances() {
		this.type = (Class<T>) BoundTypeUtil.getBindingOfSuperclass(this.getClass(), SopremoTest.class).getType();
		this.createInstances();
	}

	protected void initInstances(final T first, final T second, final T... more) {
		this.first = first;
		this.second = second;
		this.more = more;
	}

	protected void initVerifier(final EqualsVerifier<T> equalVerifier) {
		final BitSet blackBitSet = new BitSet();
		blackBitSet.set(1);

		equalVerifier.suppress(Warning.NULL_FIELDS)
			.suppress(Warning.NONFINAL_FIELDS)
			.withPrefabValues(BitSet.class, new BitSet(), blackBitSet)
			.usingGetClass();
	}

	/**
	 * Checks the following properties with {@link EqualsVerifier}:
	 * <ul>
	 * <li>Preconditions for EqualsVerifier itself.
	 * <li>Reflexivity and symmetry of the equals method.
	 * <li>Symmetry and transitivity of the equals method within an inheritance hierarchy, when applicable.
	 * <li>Consistency (by repeatedly calling equals).
	 * <li>"Non-nullity".
	 * <li>That equals, hashCode and toString not be able to throw NullPointerException. (Optional)
	 * <li>The hashCode contract.
	 * <li>That equals and hashCode be defined in terms of the same fields.
	 * <li>Immutability of the fields in terms of which equals and hashCode are defined. (Optional)
	 * <li>The finality of the fields in terms of which equals and hashCode are defined. (Optional)
	 * <li>Finality of the class under test and of the equals method itself, when applicable.
	 * </ul>
	 */
	@Test
	public void shouldComplyEqualsContract() {
		this.shouldComplyEqualsContract(this.first, this.second, this.more);
	}

	/**
	 * Checks the following properties with {@link EqualsVerifier}:
	 * <ul>
	 * <li>Preconditions for EqualsVerifier itself.
	 * <li>Reflexivity and symmetry of the equals method.
	 * <li>Symmetry and transitivity of the equals method within an inheritance hierarchy, when applicable.
	 * <li>Consistency (by repeatedly calling equals).
	 * <li>"Non-nullity".
	 * <li>That equals, hashCode and toString not be able to throw NullPointerException. (Optional)
	 * <li>The hashCode contract.
	 * <li>That equals and hashCode be defined in terms of the same fields.
	 * <li>Immutability of the fields in terms of which equals and hashCode are defined. (Optional)
	 * <li>The finality of the fields in terms of which equals and hashCode are defined. (Optional)
	 * <li>Finality of the class under test and of the equals method itself, when applicable.
	 * </ul>
	 * 
	 * @param first
	 *        An instance of T
	 * @param second
	 *        Another instance of T, which is unequal to {@code first}
	 * @param more
	 *        More instances of T, all of which are unequal to one
	 *        another and to {@code first} and {@code second}. May also
	 *        contain instances of subclasses of T
	 */
	public void shouldComplyEqualsContract(final T first, final T second, final T... more) {
		final EqualsVerifier<T> equalVerifier = EqualsVerifier.forExamples(first, second, more);
		this.initVerifier(equalVerifier);
		equalVerifier.verify();
	}

	public static ArrayNode createArrayNode(final Object... constants) {
		return JsonUtil.OBJECT_MAPPER.valueToTree(constants);
	}

	public static CompactArrayNode createCompactArray(final Object... constants) {
		final JsonNode[] nodes = new JsonNode[constants.length];
		for (int index = 0; index < nodes.length; index++)
			nodes[index] = createValueNode(constants[index]);
		return JsonUtil.asArray(nodes);
	}

	public static ObjectNode createObjectNode(final Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException("must have an even number of params");
		final ObjectNode objectNode = JsonUtil.NODE_FACTORY.objectNode();
		for (int index = 0; index < fields.length; index += 2)
			objectNode.put(fields[index].toString(), JsonUtil.OBJECT_MAPPER.valueToTree(fields[index + 1]));
		return objectNode;
	}

	public static PactJsonObject createPactJsonArray(final Object... constants) {
		return new PactJsonObject(createArrayNode(constants));
	}

	public static PactJsonObject createPactJsonObject(final Object... fields) {
		return new PactJsonObject(createObjectNode(fields));
	}

	public static PactJsonObject createPactJsonValue(final Object value) {
		return new PactJsonObject(createValueNode(value));
	}

	public static PathExpression createPath(final List<String> parts) {
		final List<EvaluationExpression> fragments = new ArrayList<EvaluationExpression>();
		for (int index = 0; index < parts.size(); index++) {
			EvaluationExpression segment;
			final String part = parts.get(index);
			if (part.equals("$"))
				segment = new InputSelection(0);
			else if (part.matches("[0-9]+"))
				segment = new InputSelection(Integer.parseInt(part));
			else if (part.matches("\\[.*\\]")) {
				if (part.charAt(1) == '*') {
					segment = new ArrayProjection(createPath(parts.subList(index + 1, parts.size())));
					index = parts.size();
				} else if (part.contains(":")) {
					final int delim = part.indexOf(":");
					segment = new ArrayAccess(Integer.parseInt(part.substring(1, delim)),
						Integer.parseInt(part.substring(delim + 1, part.length() - 1)));
				} else
					segment = new ArrayAccess(Integer.parseInt(part.substring(1, part.length() - 1)));
			} else
				segment = new ObjectAccess(part);
			fragments.add(segment);
		}
		return new PathExpression(fragments);
	}

	public static PathExpression createPath(final String... parts) {
		return createPath(Arrays.asList(parts));
	}

	public static StreamArrayNode createStreamArray(final Object... constants) {
		return StreamArrayNode.valueOf(createArrayNode(constants).getElements(), true);
	}

	public static JsonNode createValueNode(final Object value) {
		return JsonUtil.OBJECT_MAPPER.valueToTree(value);
	}

	public static String getResourcePath(final String resource) {
		try {
			final Enumeration<URL> resources = SopremoTestPlan.class.getClassLoader().getResources(resource);
			if (resources.hasMoreElements())
				return resources.nextElement().toString();
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
		throw new IllegalArgumentException("no resources found");
	}
}
