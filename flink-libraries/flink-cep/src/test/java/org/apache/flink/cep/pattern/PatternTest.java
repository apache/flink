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

package org.apache.flink.cep.pattern;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichAndCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for constructing {@link Pattern}. */
class PatternTest {

    /** These test simply test that the pattern construction completes without failure. */
    @Test
    void testStrictContiguity() {
        Pattern<Object, ?> pattern = Pattern.begin("start").next("next").next("end");
        Pattern<Object, ?> previous;
        Pattern<Object, ?> previous2;

        assertThat(previous = pattern.getPrevious()).isNotNull();
        assertThat(previous2 = previous.getPrevious()).isNotNull();
        assertThat(previous2.getPrevious()).isNull();

        assertThat(pattern.getName()).isEqualTo("end");
        assertThat(previous.getName()).isEqualTo("next");
        assertThat(previous2.getName()).isEqualTo("start");
    }

    @Test
    void testNonStrictContiguity() {
        Pattern<Object, ?> pattern = Pattern.begin("start").followedBy("next").followedBy("end");
        Pattern<Object, ?> previous;
        Pattern<Object, ?> previous2;

        assertThat(previous = pattern.getPrevious()).isNotNull();
        assertThat(previous2 = previous.getPrevious()).isNotNull();
        assertThat(previous2.getPrevious()).isNull();

        assertThat(pattern.getQuantifier().getConsumingStrategy())
                .isEqualTo(ConsumingStrategy.SKIP_TILL_NEXT);
        assertThat(previous.getQuantifier().getConsumingStrategy())
                .isEqualTo(ConsumingStrategy.SKIP_TILL_NEXT);

        assertThat(pattern.getName()).isEqualTo("end");
        assertThat(previous.getName()).isEqualTo("next");
        assertThat(previous2.getName()).isEqualTo("start");
    }

    @Test
    void testStrictContiguityWithCondition() {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .next("next")
                        .where(SimpleCondition.of(value -> value.getName().equals("foobar")))
                        .next("end")
                        .where(SimpleCondition.of(value -> value.getId() == 42));

        Pattern<Event, ?> previous;
        Pattern<Event, ?> previous2;

        assertThat(previous = pattern.getPrevious()).isNotNull();
        assertThat(previous2 = previous.getPrevious()).isNotNull();
        assertThat(previous2.getPrevious()).isNull();

        assertThat(pattern.getCondition()).isNotNull();
        assertThat(previous.getCondition()).isNotNull();
        assertThat(previous2.getCondition()).isNotNull();

        assertThat(pattern.getName()).isEqualTo("end");
        assertThat(previous.getName()).isEqualTo("next");
        assertThat(previous2.getName()).isEqualTo("start");
    }

    @Test
    void testPatternWithSubtyping() {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .next("subevent")
                        .subtype(SubEvent.class)
                        .followedBy("end");

        Pattern<Event, ?> previous;
        Pattern<Event, ?> previous2;

        assertThat(previous = pattern.getPrevious()).isNotNull();
        assertThat(previous2 = previous.getPrevious()).isNotNull();
        assertThat(previous2.getPrevious()).isNull();

        assertThat(previous.getCondition()).isNotNull();
        assertThat(previous.getCondition()).isInstanceOf(SubtypeCondition.class);

        assertThat(pattern.getName()).isEqualTo("end");
        assertThat(previous.getName()).isEqualTo("subevent");
        assertThat(previous2.getName()).isEqualTo("start");
    }

    @Test
    void testPatternWithSubtypingAndFilter() {
        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start")
                        .next("subevent")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> false))
                        .followedBy("end");

        Pattern<Event, ?> previous;
        Pattern<Event, ?> previous2;

        assertThat(previous = pattern.getPrevious()).isNotNull();
        assertThat(previous2 = previous.getPrevious()).isNotNull();
        assertThat(previous2.getPrevious()).isNull();

        assertThat(pattern.getQuantifier().getConsumingStrategy())
                .isEqualTo(ConsumingStrategy.SKIP_TILL_NEXT);
        assertThat(previous.getCondition()).isNotNull();

        assertThat(pattern.getName()).isEqualTo("end");
        assertThat(previous.getName()).isEqualTo("subevent");
        assertThat(previous2.getName()).isEqualTo("start");
    }

    @Test
    void testPatternWithOrFilter() {
        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> false))
                        .or(SimpleCondition.of(value -> false))
                        .next("or")
                        .or(SimpleCondition.of(value -> false))
                        .followedBy("end");

        Pattern<Event, ?> previous;
        Pattern<Event, ?> previous2;

        assertThat(previous = pattern.getPrevious()).isNotNull();
        assertThat(previous2 = previous.getPrevious()).isNotNull();
        assertThat(previous2.getPrevious()).isNull();

        assertThat(pattern.getQuantifier().getConsumingStrategy())
                .isEqualTo(ConsumingStrategy.SKIP_TILL_NEXT);
        assertThat(previous.getCondition()).isNotInstanceOf(RichOrCondition.class);
        assertThat(previous2.getCondition()).isInstanceOf(RichOrCondition.class);

        assertThat(pattern.getName()).isEqualTo("end");
        assertThat(previous.getName()).isEqualTo("or");
        assertThat(previous2.getName()).isEqualTo("start");
    }

    @Test
    void testRichCondition() {
        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start")
                        .where(mock(IterativeCondition.class))
                        .where(mock(IterativeCondition.class))
                        .followedBy("end")
                        .where(mock(IterativeCondition.class))
                        .or(mock(IterativeCondition.class));
        assertThat(pattern.getCondition()).isInstanceOf(RichOrCondition.class);
        assertThat(pattern.getPrevious().getCondition()).isInstanceOf(RichAndCondition.class);
    }

    @Test
    void testPatternTimesNegativeTimes() {
        assertThatThrownBy(() -> Pattern.begin("start").where(dummyCondition()).times(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPatternTimesNegativeFrom() {
        assertThatThrownBy(() -> Pattern.begin("start").where(dummyCondition()).times(-1, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPatternCanHaveQuantifierSpecifiedOnce1() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .oneOrMore()
                                        .oneOrMore()
                                        .optional())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testPatternCanHaveQuantifierSpecifiedOnce2() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .oneOrMore()
                                        .optional()
                                        .times(1))
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testPatternCanHaveQuantifierSpecifiedOnce3() {
        assertThatThrownBy(
                        () -> Pattern.begin("start").where(dummyCondition()).times(1).oneOrMore())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testPatternCanHaveQuantifierSpecifiedOnce4() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .oneOrMore()
                                        .oneOrMore())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testPatternCanHaveQuantifierSpecifiedOnce5() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .oneOrMore()
                                        .oneOrMore()
                                        .optional())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testNotNextCannotBeOneOrMore() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .notNext("not")
                                        .where(dummyCondition())
                                        .oneOrMore())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testNotNextCannotBeTimes() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .notNext("not")
                                        .where(dummyCondition())
                                        .times(3))
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testNotNextCannotBeOptional() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .notNext("not")
                                        .where(dummyCondition())
                                        .optional())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testNotFollowedCannotBeOneOrMore() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .notFollowedBy("not")
                                        .where(dummyCondition())
                                        .oneOrMore())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testNotFollowedCannotBeTimes() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .notFollowedBy("not")
                                        .where(dummyCondition())
                                        .times(3))
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testNotFollowedCannotBeOptional() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .notFollowedBy("not")
                                        .where(dummyCondition())
                                        .optional())
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testUntilCannotBeAppliedToTimes() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .times(1)
                                        .until(dummyCondition()))
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testUntilCannotBeAppliedToSingleton() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .until(dummyCondition()))
                .isInstanceOf(MalformedPatternException.class);
    }

    @Test
    void testUntilCannotBeAppliedTwice() {
        assertThatThrownBy(
                        () ->
                                Pattern.begin("start")
                                        .where(dummyCondition())
                                        .until(dummyCondition())
                                        .until(dummyCondition()))
                .isInstanceOf(MalformedPatternException.class);
    }

    private SimpleCondition<Object> dummyCondition() {
        return SimpleCondition.of(value -> true);
    }
}
