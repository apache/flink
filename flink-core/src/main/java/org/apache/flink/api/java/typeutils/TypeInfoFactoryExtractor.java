package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.buildTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.resolveTypeFromTypeHierachy;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This extractor use to extract the {@link TypeInformation} for the class that has the annotation
 * {@link org.apache.flink.api.common.typeinfo.TypeInfo}.
 * @param <T>
 */
public class TypeInfoFactoryExtractor<T> implements TypeInformationExtractor<T> {


	private final TypeInfoFactory<T> typeInfoFactory;

	private final Type factoryType;

	public TypeInfoFactoryExtractor(TypeInfoFactory<T> typeInfoFactory, Type factoryType) {
		this.typeInfoFactory = typeInfoFactory;
		this.factoryType = factoryType;
	}

	@Override
	public List<Type> getTypeHandled() {
		throw new UnsupportedOperationException(String.format("%s does not support getTypeHandled.", getClass()));
	}

	@Override
	public TypeInformation<? extends T> create(Type type, Context context) {
		final List<Type> factoryHierarchy = new ArrayList<>(Arrays.asList(type));
		factoryHierarchy.addAll(buildTypeHierarchy(typeToClass(type), typeToClass(factoryType)));

		final Type factoryDefiningType = resolveTypeFromTypeHierachy(factoryHierarchy.get(factoryHierarchy.size() - 1), factoryHierarchy, true);
		final Map<String, TypeInformation<?>> genericParams;

		if (factoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final ParameterizedType paramDefiningType = (ParameterizedType) factoryDefiningType;
			final Type[] args = typeToClass(paramDefiningType).getTypeParameters();

			final TypeInformation<?>[] subtypeInfo = createSubTypesInfo(paramDefiningType, context);
			assert subtypeInfo != null;
			for (int i = 0; i < subtypeInfo.length; i++) {
				genericParams.put(args[i].toString(), subtypeInfo[i]);
			}
		} else {
			genericParams = Collections.emptyMap();
		}

		final TypeInformation<? extends T> createdTypeInfo = typeInfoFactory.createTypeInfo(type, genericParams);

		if (createdTypeInfo == null) {
			throw new InvalidTypesException("TypeInfoFactory returned invalid TypeInformation 'null'");
		}
		return createdTypeInfo;
	}

	private TypeInformation<?>[] createSubTypesInfo(final ParameterizedType definingType, final Context context) {

		final int typeArgumentsLength = definingType.getActualTypeArguments().length;
		final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];
		for (int i = 0; i < typeArgumentsLength; i++) {
			final Type acutalTypeArgument = definingType.getActualTypeArguments()[i];
			try {
				subTypesInfo[i] = context.create(acutalTypeArgument);
			} catch (InvalidTypesException e) {
				subTypesInfo[i] = null;
			}
		}
		return subTypesInfo;
	}
}
