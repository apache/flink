package org.apache.flink.connector.pulsar.table.testutils;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * A test POJO class used by table integration tests to validate the JSON and AVRO schema are
 * compatible with corresponding Flink formats
 */
public class TestingUser implements Serializable {
    private static final long serialVersionUID = -1123545861004770003L;
    public String name;
    public Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestingUser that = (TestingUser) o;
        return Objects.equals(name, that.name) && Objects.equals(age, that.age);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age);
    }

    public static TestingUser createRandomUser() {
        TestingUser user = new TestingUser();
        user.setName(randomAlphabetic(5));
        user.setAge(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        return user;
    }
}
