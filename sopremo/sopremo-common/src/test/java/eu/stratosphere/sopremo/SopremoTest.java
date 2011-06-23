package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.ArrayMap;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.util.reflect.BoundTypeUtil;

@Ignore
public abstract class SopremoTest<T> {
	protected T first, second;

	protected T[] more;

	protected Class<T> type;

	@Before
	public void initInstances() {
		type = (Class<T>) BoundTypeUtil.getBindingOfSuperclass(getClass(), SopremoTest.class).getType();
		createInstances();
	}

	protected void createInstances() {
		initInstances(createDefaultInstance(0), createDefaultInstance(1), createDefaultInstance(2));
	}

	protected void initInstances(T first, T second, T... more) {
		this.first = first;
		this.second = second;
		this.more = more;
	}

	protected T createDefaultInstance(int index) {
		try {
			return type.newInstance();
		} catch (Exception e) {
			return null;
		}
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
		shouldComplyEqualsContract(first, second, more);
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
	public void shouldComplyEqualsContract(T first, T second, T... more) {
		BitSet blackBitSet = new BitSet();
		blackBitSet.set(1);
		EqualsVerifier
			.forExamples(first, second, more)
			.suppress(Warning.NULL_FIELDS)
			.suppress(Warning.NONFINAL_FIELDS)
			.withPrefabValues(BitSet.class, new BitSet(), blackBitSet)
			.usingGetClass().verify();
	}

	public static PathExpression createPath(String... parts) {
		return createPath(Arrays.asList(parts));
	}

	public static PathExpression createPath(List<String> parts) {
		List<EvaluableExpression> fragments = new ArrayList<EvaluableExpression>();
		for (int index = 0; index < parts.size(); index++) {
			EvaluableExpression segment;
			String part = parts.get(index);
			if (part.equals("$"))
				segment = new InputSelection(0);
			else if (part.matches("[0-9]+"))
				segment = new InputSelection(Integer.parseInt(part));
			else if (part.matches("\\[.*\\]")) {
				if (part.charAt(1) == '*') {
					segment = new ArrayMap(createPath(parts.subList(index + 1, parts.size())));
					index = parts.size();
				} else if (part.contains(":")) {
					int delim = part.indexOf(":");
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

	public static PactJsonObject createPactJsonObject(Object... fields) {
		return new PactJsonObject(createObjectNode(fields));
	}

	public static ObjectNode createObjectNode(Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException("must have an even number of params");
		ObjectNode objectNode = JsonUtil.NODE_FACTORY.objectNode();
		for (int index = 0; index < fields.length; index += 2)
			objectNode.put(fields[index].toString(), JsonUtil.OBJECT_MAPPER.valueToTree(fields[index + 1]));
		return objectNode;
	}

	public static JsonNode createStreamArray(Object... constants) {
		return StreamArrayNode.valueOf(createArrayNode(constants).getElements(), true);
	}

	public static PactJsonObject createPactJsonArray(Object... constants) {
		return new PactJsonObject(createArrayNode(constants));
	}

	public static JsonNode createArrayNode(Object... constants) {
		return JsonUtil.OBJECT_MAPPER.valueToTree(constants);
	}

	public static PactJsonObject createPactJsonValue(Object value) {
		return new PactJsonObject(createValueNode(value));
	}

	public static JsonNode createValueNode(Object value) {
		return JsonUtil.OBJECT_MAPPER.valueToTree(value);
	}
}
