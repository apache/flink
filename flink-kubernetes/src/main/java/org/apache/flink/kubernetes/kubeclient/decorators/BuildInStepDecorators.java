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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Return the build-in StepDecorators before create JobManager and TaskManager resources. */
public class BuildInStepDecorators {

    private static final BuildInStepDecorators INSTANCE = new BuildInStepDecorators();
    private Map<Integer, Class<KubernetesStepDecorator>> jobManagerStepDecorators;
    private Map<Integer, Class<KubernetesStepDecorator>> taskManagerStepDecorators;

    public static BuildInStepDecorators getInstance() {
        return INSTANCE;
    }

    public BuildInStepDecorators() {
        this.jobManagerStepDecorators =
                Stream.of(
                                new Object[][] {
                                    {1, InitJobManagerDecorator.class},
                                    {2, EnvSecretsDecorator.class},
                                    {3, MountSecretsDecorator.class},
                                    {4, CmdJobManagerDecorator.class},
                                    {5, InternalServiceDecorator.class},
                                    {6, ExternalServiceDecorator.class},
                                    {7, HadoopConfMountDecorator.class},
                                    {8, KerberosMountDecorator.class},
                                    {9, FlinkConfMountDecorator.class},
                                    {10, PodTemplateMountDecorator.class},
                                })
                        .collect(
                                Collectors.toMap(
                                        data -> (Integer) data[0],
                                        data -> (Class<KubernetesStepDecorator>) data[1]));

        this.taskManagerStepDecorators =
                Stream.of(
                                new Object[][] {
                                    {1, InitTaskManagerDecorator.class},
                                    {2, EnvSecretsDecorator.class},
                                    {3, MountSecretsDecorator.class},
                                    {4, CmdTaskManagerDecorator.class},
                                    {5, HadoopConfMountDecorator.class},
                                    {6, KerberosMountDecorator.class},
                                    {7, FlinkConfMountDecorator.class},
                                })
                        .collect(
                                Collectors.toMap(
                                        data -> (Integer) data[0],
                                        data -> (Class<KubernetesStepDecorator>) data[1]));
    }

    public List<KubernetesStepDecorator> loadDecorators(
            AbstractKubernetesParameters abstractKubernetesParameters) {
        List<KubernetesStepDecorator> decorators = new ArrayList<>();
        List<String> kubernetesDecoratorExclude =
                abstractKubernetesParameters.getKubernetesDecoratorExclude();
        Map<Integer, Class<KubernetesStepDecorator>> targetMap = null;
        Class parameterClass = null;
        if (abstractKubernetesParameters instanceof KubernetesJobManagerParameters) {
            targetMap = this.jobManagerStepDecorators;
            parameterClass = KubernetesJobManagerParameters.class;
        } else if (abstractKubernetesParameters instanceof KubernetesTaskManagerParameters) {
            targetMap = this.taskManagerStepDecorators;
            parameterClass = KubernetesTaskManagerParameters.class;
        }
        Class finalParameterClass = parameterClass;
        targetMap.entrySet().stream()
                .sorted(Map.Entry.<Integer, Class<KubernetesStepDecorator>>comparingByKey())
                .forEach(
                        e -> {
                            if (!kubernetesDecoratorExclude.contains(e.getValue().getName())) {
                                Class<KubernetesStepDecorator> value = e.getValue();
                                Constructor<KubernetesStepDecorator> constructor = null;
                                KubernetesStepDecorator kubernetesStepDecorator = null;
                                try {
                                    constructor =
                                            value.getConstructor(
                                                    AbstractKubernetesParameters.class);
                                    kubernetesStepDecorator =
                                            constructor.newInstance(abstractKubernetesParameters);
                                } catch (NoSuchMethodException noSuchMethodException) {
                                    try {
                                        constructor = value.getConstructor(finalParameterClass);
                                        kubernetesStepDecorator =
                                                constructor.newInstance(
                                                        finalParameterClass.cast(
                                                                abstractKubernetesParameters));
                                    } catch (NoSuchMethodException suchMethodException) {
                                        suchMethodException.printStackTrace();
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                } catch (Exception exc) {
                                    exc.printStackTrace();
                                }
                                decorators.add(kubernetesStepDecorator);
                            }
                        });
        return decorators;
    }
}
