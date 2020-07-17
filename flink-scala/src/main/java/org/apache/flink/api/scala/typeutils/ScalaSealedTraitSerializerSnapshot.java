package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * A {@link TypeSerializerSnapshot} for the Scala {@link SealedTraitSerializer}.
 */
public class ScalaSealedTraitSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<T, SealedTraitSerializer<T>> {

	public static final int VERSION = 1;

	Class<?>[] subtypeClasses;
	SealedTraitSerializer<T> instance;

	public ScalaSealedTraitSerializerSnapshot() {
		super(SealedTraitSerializer.class);
	}

	public ScalaSealedTraitSerializerSnapshot(SealedTraitSerializer<T> instance) {
		super(instance);
		this.instance = instance;
	}

	@Override
	public void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeInt(instance.subtypeClasses().length);
		for (Class<?> clazz : instance.subtypeClasses()) {
			out.writeUTF(clazz.getName());
		}
	}

	@Override
	public void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		int subtypeCount = in.readInt();
		subtypeClasses = new Class<?>[subtypeCount];
		for (int i = 0; i < subtypeCount; i++) {
			subtypeClasses[i] = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
		}
	}

	@Override
	public SealedTraitSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		return new SealedTraitSerializer<T>(subtypeClasses, (TypeSerializer<T>[]) nestedSerializers);
	}

	@Override
	public TypeSerializer<?>[] getNestedSerializers(SealedTraitSerializer<T> outerSerializer) {
		return outerSerializer.subtypeSerializers();
	}

	@Override
	public int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	public OuterSchemaCompatibility resolveOuterSchemaCompatibility(SealedTraitSerializer<T> newSerializer) {
		int sameClasses = Arrays.compare(subtypeClasses, newSerializer.subtypeClasses(), new Comparator<Class<?>>() {
			@Override
			public int compare(Class<?> aClass, Class<?> t1) {
				return aClass.equals(t1) ? 0 : -1;
			}
		});
		return sameClasses == 0 ? OuterSchemaCompatibility.COMPATIBLE_AS_IS : OuterSchemaCompatibility.INCOMPATIBLE;
	}
}
