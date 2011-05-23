package eu.stratosphere.sopremo.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;

public class PactUtil {

	public static Contract addKeyExtraction(PactModule module, EvaluableExpression expr, EvaluationContext context) {
		MapContract<PactNull, PactJsonObject, Key, PactJsonObject> selectionMap =
			new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(KeyExtractionStub.class);
		int inputIndex = 0;
		if (expr instanceof Path) {
			inputIndex = getInputIndex((Path) expr);
			expr = Path.replace((Path) expr, new Path(new Input(inputIndex)), new Path());
		}
		selectionMap.setInput(module.getInput(inputIndex));
		PactUtil.setTransformationAndContext(selectionMap.getStubParameters(), expr, context);

		return selectionMap;
	}

	public static int getInputIndex(Path expr) {
		Evaluable fragment = expr.getFragment(0);
		if (fragment instanceof Input)
			return ((Input) fragment).getIndex();
		return 0;
	}

	// public static void setEvaluableExpression(Configuration config, String key, Evaluable transformation) {
	// config.setString(key, objectToString(transformation));
	// }
	//
	// public static Evaluable getEvaluableExpression(Configuration config, String key) {
	// String string = config.getString(key, null);
	// if (string == null)
	// return null;
	// return (Evaluable) stringToObject(string);
	// }

	public static Object stringToObject(String string) {
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

	public static String objectToString(Object transformation) {
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

	public static void setObject(Configuration config, String key, Object object) {
		config.setString(key, objectToString(object));
	}

	@SuppressWarnings("unchecked")
	public static <T> T getObject(Configuration config, String key, Class<T> objectClass) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (T) stringToObject(string);
	}

	public static void setTransformationAndContext(Configuration config, Evaluable transformation,
			EvaluationContext context) {
		setObject(config, "transformation", transformation);
		setObject(config, "context", context);
	}
}
