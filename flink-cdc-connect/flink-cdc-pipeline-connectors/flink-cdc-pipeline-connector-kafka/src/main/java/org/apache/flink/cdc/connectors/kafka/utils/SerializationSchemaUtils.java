/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafka.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;

/** Utils for creating JsonRowDataSerializationSchema. */
public class SerializationSchemaUtils {

    public static JsonRowDataSerializationSchema createSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean ignoreNullFields) {
        try {
            Class<?>[] fullParams =
                    new Class[] {
                        RowType.class,
                        TimestampFormat.class,
                        JsonFormatOptions.MapNullKeyMode.class,
                        String.class,
                        boolean.class,
                        boolean.class // Flink 1.20
                    };

            Object[] fullParamValues =
                    new Object[] {
                        rowType,
                        timestampFormat,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber,
                        ignoreNullFields
                    };

            Class<?> serializerClass = JsonRowDataSerializationSchema.class;

            for (int i = fullParams.length; i >= 5; i--) {
                try {
                    Constructor<?> constructor =
                            serializerClass.getConstructor(Arrays.copyOfRange(fullParams, 0, i));

                    return (JsonRowDataSerializationSchema)
                            constructor.newInstance(Arrays.copyOfRange(fullParamValues, 0, i));

                } catch (NoSuchMethodException ignored) {
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create JsonRowDataSerializationSchema", e);
        }
        throw new RuntimeException(
                "Failed to find appropriate constructor for JsonRowDataSerializationSchema");
    }

    public static boolean ignoreNullFields(ReadableConfig formatOptions) {
        try {
            Field field = JsonFormatOptions.class.getField("ENCODE_DECIMAL_AS_PLAIN_NUMBER");
            ConfigOption<Boolean> encodeOption = (ConfigOption<Boolean>) field.get(null);
            return formatOptions.get(encodeOption);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return false;
        }
    }
}
