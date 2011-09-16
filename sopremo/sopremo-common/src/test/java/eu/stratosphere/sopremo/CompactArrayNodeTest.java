package eu.stratosphere.sopremo;
import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createCompactArray;
import static eu.stratosphere.sopremo.JsonUtil.createValueNode;

import java.util.Arrays;
import java.util.List;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Ignore;

import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

@Ignore
public class CompactArrayNodeTest extends SopremoTest<ArrayNode> {
	@Override
	protected ArrayNode createDefaultInstance(final int index) {
		return createCompactArray(index);
	}

	@Override
	protected void initVerifier(final EqualsVerifier<ArrayNode> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier
			.withPrefabValues(List.class, Arrays.asList(createPactJsonValue("red")),
				Arrays.asList(createPactJsonValue("black")))
			.withPrefabValues(ArrayNode.class, createCompactArray("red"), createCompactArray("black"))
			.withPrefabValues(ContainerNode.class, createArrayNode("red"), createArrayNode("black"))
			.withPrefabValues(JsonNode.class, createValueNode("red"), createValueNode("black"));
	}

	@Override
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract();
	}
}
