package eu.stratosphere.sopremo.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;

public class Projection extends Operator {

	public static class ProjectionStub extends MapStub<PactNull, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
			JsonNode transformedObject = transformation.evaluate(value.getValue());
			out.collect(key, new PactJsonObject(transformedObject));
		}

	}

	public Projection(Transformation transformation, Operator input) {
		super(transformation, input);
	}

	@Override
	public PactModule asPactModule() {
		PactModule module = new PactModule(1, 1);
		MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject> projectionMap = new MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject>(
			ProjectionStub.class);
		module.getOutput(0).setInput(projectionMap);
		projectionMap.setInput(module.getInput(0));
		setTransformation(projectionMap.getStubParameters(), "transformation", getTransformation());
		return module;
	}

	protected static void setTransformation(Configuration config, String key, Transformation transformation) {
		config.setString(key, objectToString(transformation));
	}

	protected static Transformation getTransformation(Configuration config, String key) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (Transformation) stringToObject(string);
	}

	private static Object stringToObject(String string) {
		Object object = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(string
				.getBytes())));
			object = in.readObject();
			in.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}

	private static String objectToString(Transformation transformation) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream out = new ObjectOutputStream(bos);
			out.writeObject(transformation);
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		String string = new String(Base64.encodeBase64(bos.toByteArray()));
		return string;
	}

}
