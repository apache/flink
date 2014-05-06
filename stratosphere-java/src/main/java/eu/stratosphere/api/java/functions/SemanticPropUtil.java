package eu.stratosphere.api.java.functions;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFields;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class SemanticPropUtil {
	public SingleInputSemanticProperties getSemanticPropsSingle(Set<Annotation> set, TypeInformation<?> inType, TypeInformation<?> outType) {
		Iterator<Annotation> it = set.iterator();
		SingleInputSemanticProperties result = null;
		
		while (it.hasNext()) {
			if (result == null) {
				result = new SingleInputSemanticProperties();
			}
			
			Annotation ann = it.next();
			
			if (ann instanceof ConstantFields) {
				ConstantFields cf = (ConstantFields) ann;
			} else if (ann instanceof ConstantFieldsExcept) {
				ConstantFieldsExcept cfe = (ConstantFieldsExcept) ann;
			} else if (ann instanceof ReadFields) {
				ReadFields rf = (ReadFields) ann;
			}
		}
		return null;
	}
	
	private void parseConstantFields(ConstantFields cf, SingleInputSemanticProperties sm) {
		
	}
	
	private void parseConstantFieldsExcept(ConstantFieldsExcept cfe, SingleInputSemanticProperties sm) {
		
	}
	
	private void parseReadFields(ReadFields rf, SingleInputSemanticProperties sm) {
	
	}
	
	public DualInputSemanticProperties getSemanticPropsDua(Set<Annotation> set, TypeInformation<?> inType1, TypeInformation<?> inType2, TypeInformation<?> outType) {
		return null;
	}
}
