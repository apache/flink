/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.interpreter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Copied to fix calcite issues. This class should be removed together with upgrade Janino to
 * 3.1.9+(https://issues.apache.org/jira/browse/FLINK-27995). FLINK modifications are at lines
 *
 * <ol>
 *   <li>Line 229 ~ 235
 *   <li>Line 239 ~ 241
 * </ol>
 */
public class JaninoRexCompiler implements Interpreter.ScalarCompiler {
    private final RexBuilder rexBuilder;

    public JaninoRexCompiler(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    @Override
    public Scalar.Producer compile(List<RexNode> nodes, RelDataType inputRowType) {
        final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }
        final RexProgram program = programBuilder.getProgram();

        final BlockBuilder list = new BlockBuilder();
        final BlockBuilder staticList = new BlockBuilder().withRemoveUnused(false);
        final ParameterExpression context_ = Expressions.parameter(Context.class, "context");
        final ParameterExpression outputValues_ =
                Expressions.parameter(Object[].class, "outputValues");
        final JavaTypeFactoryImpl javaTypeFactory =
                new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

        // public void execute(Context, Object[] outputValues)
        final RexToLixTranslator.InputGetter inputGetter =
                new RexToLixTranslator.InputGetterImpl(
                        Expressions.field(context_, BuiltInMethod.CONTEXT_VALUES.field),
                        PhysTypeImpl.of(javaTypeFactory, inputRowType, JavaRowFormat.ARRAY, false));
        final Function1<String, RexToLixTranslator.InputGetter> correlates =
                a0 -> {
                    throw new UnsupportedOperationException();
                };
        final Expression root = Expressions.field(context_, BuiltInMethod.CONTEXT_ROOT.field);
        final SqlConformance conformance =
                SqlConformanceEnum.DEFAULT; // TODO: get this from implementor
        final List<Expression> expressionList =
                RexToLixTranslator.translateProjects(
                        program,
                        javaTypeFactory,
                        conformance,
                        list,
                        staticList,
                        null,
                        root,
                        inputGetter,
                        correlates);
        Ord.forEach(
                expressionList,
                (expression, i) ->
                        list.add(
                                Expressions.statement(
                                        Expressions.assign(
                                                Expressions.arrayIndex(
                                                        outputValues_, Expressions.constant(i)),
                                                expression))));
        return baz(context_, outputValues_, list.toBlock(), staticList.toBlock().statements);
    }

    /**
     * Given a method that implements {@link Scalar#execute(Context, Object[])}, adds a bridge
     * method that implements {@link Scalar#execute(Context)}, and compiles.
     */
    static Scalar.Producer baz(
            ParameterExpression context_,
            ParameterExpression outputValues_,
            BlockStatement block,
            List<Statement> declList) {
        final List<MemberDeclaration> declarations = new ArrayList<>();
        final List<MemberDeclaration> innerDeclarations = new ArrayList<>();

        // public Scalar apply(DataContext root) {
        //   <<staticList>>
        //   return new Scalar() {
        //     <<inner declarations>>
        //   };
        // }
        final List<Statement> statements = new ArrayList<>(declList);
        statements.add(
                Expressions.return_(
                        null,
                        Expressions.new_(Scalar.class, ImmutableList.of(), innerDeclarations)));
        declarations.add(
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        Scalar.class,
                        BuiltInMethod.FUNCTION_APPLY.method.getName(),
                        ImmutableList.of(DataContext.ROOT),
                        Expressions.block(statements)));

        // (bridge method)
        // public Object apply(Object root) {
        //   return this.apply((DataContext) root);
        // }
        final ParameterExpression objectRoot = Expressions.parameter(Object.class, "root");
        declarations.add(
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        Object.class,
                        BuiltInMethod.FUNCTION_APPLY.method.getName(),
                        ImmutableList.of(objectRoot),
                        Expressions.block(
                                Expressions.return_(
                                        null,
                                        Expressions.call(
                                                Expressions.parameter(
                                                        Scalar.Producer.class, "this"),
                                                BuiltInMethod.FUNCTION_APPLY.method,
                                                Expressions.convert_(
                                                        objectRoot, DataContext.class))))));

        // public void execute(Context, Object[] outputValues)
        innerDeclarations.add(
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        void.class,
                        BuiltInMethod.SCALAR_EXECUTE2.method.getName(),
                        ImmutableList.of(context_, outputValues_),
                        block));

        // public Object execute(Context)
        final BlockBuilder builder = new BlockBuilder();
        final Expression values_ =
                builder.append(
                        "values",
                        Expressions.newArrayBounds(Object.class, 1, Expressions.constant(1)));
        builder.add(
                Expressions.statement(
                        Expressions.call(
                                Expressions.parameter(Scalar.class, "this"),
                                BuiltInMethod.SCALAR_EXECUTE2.method,
                                context_,
                                values_)));
        builder.add(
                Expressions.return_(
                        null, Expressions.arrayIndex(values_, Expressions.constant(0))));
        innerDeclarations.add(
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        Object.class,
                        BuiltInMethod.SCALAR_EXECUTE1.method.getName(),
                        ImmutableList.of(context_),
                        builder.toBlock()));

        final ClassDeclaration classDeclaration =
                Expressions.classDecl(
                        Modifier.PUBLIC,
                        "Buzz",
                        null,
                        ImmutableList.of(Scalar.Producer.class),
                        declarations);
        String s = Expressions.toString(declarations, "\n", false);
        if (CalciteSystemProperty.DEBUG.value()) {
            Util.debugCode(System.out, s);
        }
        try {
            return getScalar(classDeclaration, s);
        } catch (CompileException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Scalar.Producer getScalar(ClassDeclaration expr, String s)
            throws CompileException, IOException {
        ICompilerFactory compilerFactory;
        // FLINK MODIFICATION BEGIN
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to instantiate java compiler", e);
        }
        // FLINK MODIFICATION END
        IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName(expr.name);
        cbe.setImplementedInterfaces(new Class[] {Scalar.Producer.class});
        // FLINK MODIFICATION BEGIN
        cbe.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());
        // FLINK MODIFICATION END
        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation(true, true, true);
        }
        return (Scalar.Producer) cbe.createInstance(new StringReader(s));
    }
}
