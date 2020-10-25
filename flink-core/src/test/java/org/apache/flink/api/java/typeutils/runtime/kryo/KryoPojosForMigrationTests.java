/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.Objects;

/**
 * POJOS needed for {@link KryoPojosForMigrationTests}.
 */
@SuppressWarnings("WeakerAccess")
public class KryoPojosForMigrationTests {

	public static abstract class Animal {
	}

	public static class Dog extends Animal {
		private final String name;

		public Dog(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Dog dog = (Dog) o;
			return Objects.equals(name, dog.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name);
		}
	}

	public static class Cat extends Animal {
		private final String name;
		private final int age;

		public Cat(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public int getAge() {
			return age;
		}
	}

	public static class Parrot extends Animal {
		private final String accent;

		public Parrot(String accent) {
			this.accent = accent;
		}

		public String getAccent() {
			return accent;
		}
	}

	// HousePets is registered explicitly in flink-1.6-kryo-type-serializer-*-* test resources.
	@SuppressWarnings("unused")
	public static class HousePets {
		private final Dog dog;
		private final Cat cat;

		public HousePets(Dog dog, Cat cat) {
			this.dog = dog;
			this.cat = cat;
		}

		public Dog getDog() {
			return dog;
		}

		public Cat getCat() {
			return cat;
		}
	}

	/**
	 * A Serializer that was registered during test data generation.
	 */
	public static class ParrotKryoSerializer extends Serializer<Parrot> implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void write(Kryo kryo, Output output, Parrot object) {
			output.writeString(object.getAccent());
		}

		@Override
		public Parrot read(Kryo kryo, Input input, Class<Parrot> type) {
			return new Parrot(input.readString());
		}
	}

	/**
	 * A Serializer that was registered during test data generation.
	 */
	public static class DogKryoSerializer extends Serializer<Dog> implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void write(Kryo kryo, Output output, Dog object) {
			output.writeString(object.getName());
		}

		@Override
		public Dog read(Kryo kryo, Input input, Class<Dog> type) {
			return new Dog(input.readString());
		}
	}

	/**
	 * A Serializer that is registered in migration tests.
	 */
	public static class DogV2KryoSerializer extends Serializer<Dog> implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void write(Kryo kryo, Output output, Dog object) {
			output.writeString(object.getName());
		}

		@Override
		public Dog read(Kryo kryo, Input input, Class<Dog> type) {
			return new Dog(input.readString());
		}
	}


}
