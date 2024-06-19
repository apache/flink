package org.apache.flink.streaming.util.watermark;

import org.apache.flink.api.common.WatermarkCombiner;
import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class WatermarkUtils {

    public static Optional<Set<WatermarkDeclaration>> getWatermarkCombiner(Object obj) {
        // Check if the object's class is "org.apache.flink.datastream.impl.operators.ProcessOperator"
        Class<?> processOperatorClass;
        try {
            processOperatorClass = Class.forName("org.apache.flink.datastream.impl.operators.ProcessOperator");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("org.apache.flink.datastream.impl.operators.ProcessOperator class not found", e);
        }

        if (processOperatorClass.isInstance(obj)) {
            try {
                // Access the "watermarkCombiner" method
                Method watermarkCombinerMethod = processOperatorClass.getDeclaredMethod("watermarkDeclarations");
                watermarkCombinerMethod.setAccessible(true);

                // Invoke the method and return the result
                return Optional.ofNullable((Set<WatermarkDeclaration>) watermarkCombinerMethod.invoke(obj));
            } catch (Exception e) {
                throw new RuntimeException("Failed to get watermark combiner", e);
            }
        } else {
            return Optional.empty();
        }
    }

    public static Set<Class<? extends WatermarkDeclaration>> getWatermarkDeclarations(StreamOperator<?> streamOperator) {
        // Check if the object's class is "org.apache.flink.datastream.impl.operators.ProcessOperator"
        Class<?> processOperatorClass;
        try {
            processOperatorClass = Class.forName("org.apache.flink.datastream.impl.operators.ProcessOperator");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("org.apache.flink.datastream.impl.operators.ProcessOperator class not found", e);
        }

        if (processOperatorClass.isInstance(streamOperator)) {
            try {
                // Access the "watermarkCombiner" method
                Method watermarkCombinerMethod = processOperatorClass.getDeclaredMethod("watermarkDeclarations");
                watermarkCombinerMethod.setAccessible(true);

                // Invoke the method and return the result
                return (Set<Class<? extends WatermarkDeclaration>>) watermarkCombinerMethod.invoke(
                        streamOperator);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get watermark combiner", e);
            }
        } else {
            return Collections.emptySet();
        }
    }
}
