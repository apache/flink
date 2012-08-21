package eu.stratosphere.sopremo.operator;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class SopremoPlanTest {
	@Test
	public void shouldBeSerializable() throws IOException {
		final SopremoPlan plan = new SopremoPlan();
		final Source input = new Source("input");
		final Sink output = new Sink("output").withInputs(input);
		plan.setSinks(output);

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SopremoUtil.serializeObject(new DataOutputStream(baos), plan);
		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final SopremoPlan deserializedPlan =
			SopremoUtil.deserializeObject(new DataInputStream(bais), SopremoPlan.class);

		Assert.assertEquals(plan, deserializedPlan);
	}
}
