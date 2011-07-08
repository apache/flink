package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.List;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ContainerNode;
import org.junit.Ignore;

@Ignore
public class CompactArrayNodeTest extends SopremoTest<CompactArrayNode> {
	@Override
	protected CompactArrayNode createDefaultInstance(int index) {
		return createCompactArray(index);
	}

	@Override
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract();
	}

	@Override
	protected void initVerifier(EqualsVerifier<CompactArrayNode> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier
			.withPrefabValues(List.class, Arrays.asList(createPactJsonValue("red")),
				Arrays.asList(createPactJsonValue("black")))
			.withPrefabValues(CompactArrayNode.class, createCompactArray("red"), createCompactArray("black"))
			.withPrefabValues(ContainerNode.class, createArrayNode("red"), createArrayNode("black"))
			.withPrefabValues(JsonNode.class, createValueNode("red"), createValueNode("black"));
	}
}
