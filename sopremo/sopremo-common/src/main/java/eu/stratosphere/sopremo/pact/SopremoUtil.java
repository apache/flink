package eu.stratosphere.sopremo.pact;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;

public class SopremoUtil {
	//
	// public static Contract addKeyExtraction(PactModule module, EvaluableExpression expr, EvaluationContext context) {
	// MapContract<PactNull, PactJsonObject, Key, PactJsonObject> extractionMap =
	// new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(KeyExtractionStub.class);
	// int inputIndex = 0;
	// if (expr instanceof PathExpression) {
	// inputIndex = getInputIndex((PathExpression) expr);
	// expr = new PathExpression(expr);
	// ((PathExpression) expr).replace(new PathExpression(new InputSelection(inputIndex)), new PathExpression());
	// } else if (expr instanceof InputSelection) {
	// inputIndex = ((InputSelection) expr).getIndex();
	// expr = EvaluableExpression.IDENTITY;
	// }
	// extractionMap.setInput(module.getInput(inputIndex));
	// SopremoUtil.setTransformationAndContext(extractionMap.getStubParameters(), expr, context);
	//
	// return extractionMap;
	// }
	//
	// public static Contract addKeyRemover(Contract input) {
	// MapContract<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> removerMap =
	// new MapContract<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject>(KeyRemover.class);
	// removerMap.setInput(input);
	// return removerMap;
	// }
	//
	// public static List<Contract> wrapInputsWithArray(PactModule module, EvaluationContext context) {
	// List<Contract> inputs = new ArrayList<Contract>();
	// int inputCount = module.getInputs().length;
	// for (int index = 0; index < inputCount; index++) {
	// MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject> arrayWrapMap = new MapContract<PactNull,
	// PactJsonObject, PactNull, PactJsonObject>(
	// ArrayWrapper.class);
	// inputs.add(arrayWrapMap);
	// arrayWrapMap.setInput(module.getInput(index));
	// SopremoUtil.setContext(arrayWrapMap.getStubParameters(), context);
	// arrayWrapMap.getStubParameters().setInteger("arraySize", inputCount);
	// arrayWrapMap.getStubParameters().setInteger("arrayIndex", index);
	// }
	// return inputs;
	// }

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

	public static int getInputIndex(ContainerExpression expr) {
		InputSelection fragment = expr.find(InputSelection.class);
		if (fragment == null)
			return 0;
		return fragment.getIndex();
	}

	public static int getInputIndex(EvaluableExpression expr) {
		if (expr instanceof ContainerExpression)
			return getInputIndex((ContainerExpression) expr);
		else if (expr instanceof InputSelection)
			return ((InputSelection) expr).getIndex();
		return 0;
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserialize(Configuration config, String key, Class<T> objectClass) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (T) stringToObject(string);
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

	public static void serialize(Configuration config, String key, Object object) {
		config.setString(key, objectToString(object));
	}

	public static void setContext(Configuration config, EvaluationContext context) {
		serialize(config, "context", context);
	}

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
}
