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

package org.apache.flink.runtime.rpc.messages;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Remote rpc invocation message which is used when the actor communication is remote.
 *
 * <p>In order to fail fast and report an appropriate error message to the user, we check that the
 * parameter types and the arguments are serializable. In case the invocation call contains a
 * non-serializable object, then an {@link IOException} is thrown.
 */
public class RemoteRpcInvocation implements RpcInvocation, Serializable {
    private static final long serialVersionUID = 1L;

    // Wrap the invocation information to ease the serialization.
    private RemoteRpcInvocation.MethodInvocation methodInvocation;

    private transient String toString;

    public RemoteRpcInvocation(
            final String declaringClassName,
            final String methodName,
            final Class<?>[] parameterTypes,
            final Object[] args)
            throws IOException {
        for (int i = 0; i < (args == null ? 0 : args.length); ++i) {
            if (args[i] != null && !(args[i] instanceof Serializable)) {
                throw new IOException(
                        "Could not serialize "
                                + i
                                + "th argument of method "
                                + methodName
                                + ". This indicates that the argument type "
                                + args.getClass().getName()
                                + " is not serializable. Arguments have to "
                                + "be serializable for remote rpc calls.");
            }
        }
        methodInvocation =
                new MethodInvocation(declaringClassName, methodName, parameterTypes, args);
    }

    @Override
    public String getMethodName() {
        return methodInvocation.getMethodName();
    }

    private String getDeclaringClassName() {
        return methodInvocation.getDeclaringClassName();
    }

    @Override
    public Class<?>[] getParameterTypes() {
        return methodInvocation.getParameterTypes();
    }

    @Override
    public Object[] getArgs() {
        return methodInvocation.getArgs();
    }

    @Override
    public String toString() {
        if (toString == null) {
            Class<?>[] parameterTypes = getParameterTypes();
            String methodName = getMethodName();
            String declaringClassName = getDeclaringClassName();

            toString =
                    "RemoteRpcInvocation("
                            + RpcInvocation.convertRpcToString(
                                    declaringClassName, methodName, parameterTypes)
                            + ")";
        }

        return toString;
    }

    // -------------------------------------------------------------------
    // Serialization methods
    // -------------------------------------------------------------------

    private void writeObject(ObjectOutputStream oos) throws IOException {
        // Translate it to byte array so that we can deserialize classes which cannot be found in
        // pekko class loader.
        byte[] bytes = InstantiationUtil.serializeObject(methodInvocation);
        oos.writeObject(bytes);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        byte[] bytes = (byte[]) ois.readObject();
        methodInvocation =
                InstantiationUtil.deserializeObject(bytes, ClassLoader.getSystemClassLoader());
    }

    // -------------------------------------------------------------------
    // Utility classes
    // -------------------------------------------------------------------

    /** Wrapper class for the method invocation information. */
    private static final class MethodInvocation implements Serializable {
        private static final long serialVersionUID = 1L;

        private String declaringClassName;
        private String methodName;
        private Class<?>[] parameterTypes;
        private Object[] args;

        private MethodInvocation(
                final String declaringClassName,
                final String methodName,
                final Class<?>[] parameterTypes,
                final Object[] args) {
            this.declaringClassName = declaringClassName;
            this.methodName = methodName;
            this.parameterTypes = Preconditions.checkNotNull(parameterTypes);
            this.args = args;
        }

        String getDeclaringClassName() {
            return declaringClassName;
        }

        String getMethodName() {
            return methodName;
        }

        Class<?>[] getParameterTypes() {
            return parameterTypes;
        }

        Object[] getArgs() {
            return args;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.writeUTF(declaringClassName);
            oos.writeUTF(methodName);

            oos.writeInt(parameterTypes.length);

            for (Class<?> parameterType : parameterTypes) {
                oos.writeObject(parameterType);
            }

            if (args != null) {
                oos.writeBoolean(true);

                for (int i = 0; i < args.length; i++) {
                    try {
                        oos.writeObject(args[i]);
                    } catch (IOException e) {
                        throw new IOException(
                                "Could not serialize "
                                        + i
                                        + "th argument of method "
                                        + methodName
                                        + ". This indicates that the argument type "
                                        + args.getClass().getName()
                                        + " is not serializable. Arguments have to "
                                        + "be serializable for remote rpc calls.",
                                e);
                    }
                }
            } else {
                oos.writeBoolean(false);
            }
        }

        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            declaringClassName = ois.readUTF();
            methodName = ois.readUTF();

            int length = ois.readInt();

            parameterTypes = new Class<?>[length];

            for (int i = 0; i < length; i++) {
                try {
                    parameterTypes[i] = (Class<?>) ois.readObject();
                } catch (IOException e) {
                    StringBuilder incompleteMethod = getIncompleteMethodString(i, 0);
                    throw new IOException(
                            "Could not deserialize "
                                    + i
                                    + "th parameter type of method "
                                    + incompleteMethod
                                    + '.',
                            e);
                } catch (ClassNotFoundException e) {
                    // note: wrapping this CNFE into another CNFE does not overwrite the Exception
                    //       stored in the ObjectInputStream (see ObjectInputStream#readSerialData)
                    // -> add a suppressed exception that adds a more specific message
                    StringBuilder incompleteMethod = getIncompleteMethodString(i, 0);
                    e.addSuppressed(
                            new ClassNotFoundException(
                                    "Could not deserialize "
                                            + i
                                            + "th "
                                            + "parameter type of method "
                                            + incompleteMethod
                                            + ". This indicates that the parameter "
                                            + "type is not part of the system class loader."));
                    throw e;
                }
            }

            boolean hasArgs = ois.readBoolean();

            if (hasArgs) {
                args = new Object[length];

                for (int i = 0; i < length; i++) {
                    try {
                        args[i] = ois.readObject();
                    } catch (IOException e) {
                        StringBuilder incompleteMethod = getIncompleteMethodString(length, i);
                        throw new IOException(
                                "Could not deserialize "
                                        + i
                                        + "th argument of method "
                                        + incompleteMethod
                                        + '.',
                                e);
                    } catch (ClassNotFoundException e) {
                        // note: wrapping this CNFE into another CNFE does not overwrite the
                        // Exception
                        //       stored in the ObjectInputStream (see
                        // ObjectInputStream#readSerialData)
                        // -> add a suppressed exception that adds a more specific message
                        StringBuilder incompleteMethod = getIncompleteMethodString(length, i);
                        e.addSuppressed(
                                new ClassNotFoundException(
                                        "Could not deserialize "
                                                + i
                                                + "th "
                                                + "argument of method "
                                                + incompleteMethod
                                                + ". This indicates that the argument "
                                                + "type is not part of the system class loader."));
                        throw e;
                    }
                }
            } else {
                args = null;
            }
        }

        private StringBuilder getIncompleteMethodString(
                int lastMethodTypeIdx, int lastArgumentIdx) {
            StringBuilder incompleteMethod = new StringBuilder();
            incompleteMethod.append(methodName).append('(');
            for (int i = 0; i < lastMethodTypeIdx; ++i) {
                incompleteMethod.append(parameterTypes[i].getCanonicalName());
                if (i < lastArgumentIdx) {
                    incompleteMethod.append(": ").append(args[i]);
                }
                incompleteMethod.append(", ");
            }
            incompleteMethod.append("...)"); // some parameters could not be deserialized
            return incompleteMethod;
        }
    }
}
