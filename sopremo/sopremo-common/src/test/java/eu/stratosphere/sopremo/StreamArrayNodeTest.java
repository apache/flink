package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;

import nl.jqno.equalsverifier.EqualsVerifier;

@Ignore
public class StreamArrayNodeTest extends SopremoTest<StreamArrayNode> {
	@Override
	protected StreamArrayNode createDefaultInstance(int index) {
		return createStreamArray(index);
	}

	@Override
	protected void initVerifier(EqualsVerifier<StreamArrayNode> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(List.class, Arrays.asList(createPactJsonValue("red")),
			Arrays.asList(createPactJsonValue("black")));
		equalVerifier.withPrefabValues(StreamArrayNode.class, createStreamArray("red"), createStreamArray("black"));
	}
}
