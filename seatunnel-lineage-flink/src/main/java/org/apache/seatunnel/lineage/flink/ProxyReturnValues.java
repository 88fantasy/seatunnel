/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.lineage.flink;

import java.lang.reflect.Method;

/**
 * Supplies the value a lineage {@link java.lang.reflect.InvocationHandler} returns for a callback
 * it does not act on.
 *
 * <p>A dynamic proxy unboxes whatever the handler returns, so returning {@code null} for a method
 * with a primitive return type throws inside the caller's frame rather than in the handler.
 */
public final class ProxyReturnValues {

    private ProxyReturnValues() {}

    /** Returns the zero value of the method's return type, or null when it is not primitive. */
    public static Object defaultFor(Method method) {
        try {
            if (method == null || method.getReturnType() == void.class) {
                return null;
            }
            Class<?> returnType = method.getReturnType();
            if (!returnType.isPrimitive()) {
                return null;
            }
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == char.class) {
                return '\0';
            }
            if (returnType == byte.class) {
                return (byte) 0;
            }
            if (returnType == short.class) {
                return (short) 0;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            if (returnType == float.class) {
                return 0F;
            }
            return 0D;
        } catch (Throwable error) {
            return null;
        }
    }
}
