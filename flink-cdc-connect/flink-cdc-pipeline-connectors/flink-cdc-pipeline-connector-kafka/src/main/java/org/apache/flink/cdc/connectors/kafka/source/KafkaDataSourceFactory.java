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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link Factory} for creating Kafka pipeline sources. */
@Internal
public class KafkaDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(KafkaDataSourceOptions.PROPERTIES_PREFIX);
        Configuration configuration = context.getFactoryConfiguration();
        boolean hasTopics = configuration.getOptional(KafkaDataSourceOptions.TOPIC).isPresent();
        boolean hasTopicPattern =
                configuration.getOptional(KafkaDataSourceOptions.TOPIC_PATTERN).isPresent();
        if (hasTopics == hasTopicPattern) {
            throw new ValidationException(
                    "Exactly one of options 'topic' and 'topic-pattern' must be configured.");
        }
        List<String> topics = Collections.emptyList();
        String topicPattern = null;
        if (hasTopics) {
            topics =
                    Arrays.stream(configuration.get(KafkaDataSourceOptions.TOPIC).split(","))
                            .map(String::trim)
                            .filter(topic -> !topic.isEmpty())
                            .collect(Collectors.toList());
            if (topics.isEmpty()) {
                throw new ValidationException("Option 'topic' must contain at least one topic.");
            }
        } else {
            topicPattern = configuration.get(KafkaDataSourceOptions.TOPIC_PATTERN).trim();
            if (topicPattern.isEmpty()) {
                throw new ValidationException("Option 'topic-pattern' must not be empty.");
            }
        }

        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
            if (entry.getKey().startsWith(KafkaDataSourceOptions.PROPERTIES_PREFIX)) {
                properties.setProperty(
                        entry.getKey().substring(KafkaDataSourceOptions.PROPERTIES_PREFIX.length()),
                        entry.getValue());
            }
        }
        configuration
                .getOptional(KafkaDataSourceOptions.GROUP_ID)
                .ifPresent(groupId -> properties.setProperty("group.id", groupId));
        if (!properties.containsKey("bootstrap.servers")) {
            throw new ValidationException(
                    "Kafka bootstrap servers must be configured with 'properties.bootstrap.servers'.");
        }
        if (!properties.containsKey("group.id")) {
            throw new ValidationException(
                    "Kafka consumer group must be configured with 'group-id' or 'properties.group.id'.");
        }

        List<String> primaryKeys =
                configuration
                        .getOptional(KafkaDataSourceOptions.PRIMARY_KEYS)
                        .map(
                                value ->
                                        parsePrimaryKeys(
                                                value, KafkaDataSourceOptions.PRIMARY_KEYS.key()))
                        .orElse(Collections.emptyList());
        Map<TableId, List<String>> primaryKeysMapping =
                configuration
                        .getOptional(KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING)
                        .map(KafkaDataSourceFactory::parsePrimaryKeysMapping)
                        .orElse(Collections.emptyMap());

        return new KafkaDataSource(
                topics,
                topicPattern,
                properties,
                configuration.get(KafkaDataSourceOptions.SCAN_STARTUP_MODE),
                primaryKeys,
                primaryKeysMapping);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        KafkaDataSourceOptions.TOPIC,
                        KafkaDataSourceOptions.TOPIC_PATTERN,
                        KafkaDataSourceOptions.GROUP_ID,
                        KafkaDataSourceOptions.PRIMARY_KEYS,
                        KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING,
                        KafkaDataSourceOptions.SCAN_STARTUP_MODE));
    }

    private static Map<TableId, List<String>> parsePrimaryKeysMapping(String value) {
        if (value.trim().isEmpty()) {
            throw malformedOption(KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING.key(), value);
        }
        Map<TableId, List<String>> mapping = new LinkedHashMap<>();
        for (String entry : value.split(";", -1)) {
            String[] tableAndKeys = entry.trim().split(":", -1);
            if (tableAndKeys.length != 2 || tableAndKeys[0].trim().isEmpty()) {
                throw malformedOption(KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING.key(), value);
            }
            TableId tableId;
            try {
                tableId = TableId.parse(tableAndKeys[0].trim());
            } catch (IllegalArgumentException e) {
                throw malformedOption(KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING.key(), value);
            }
            List<String> primaryKeys =
                    parsePrimaryKeys(
                            tableAndKeys[1], KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING.key());
            if (mapping.put(tableId, primaryKeys) != null) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' contains duplicate mapping for table '%s'.",
                                KafkaDataSourceOptions.PRIMARY_KEYS_MAPPING.key(), tableId));
            }
        }
        return mapping;
    }

    private static List<String> parsePrimaryKeys(String value, String optionName) {
        LinkedHashSet<String> primaryKeys = new LinkedHashSet<>();
        for (String primaryKey : value.split(",", -1)) {
            String trimmed = primaryKey.trim();
            if (trimmed.isEmpty()) {
                throw malformedOption(optionName, value);
            }
            if (!primaryKeys.add(trimmed)) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' contains duplicate primary key column '%s'.",
                                optionName, trimmed));
            }
        }
        return Arrays.asList(primaryKeys.toArray(new String[0]));
    }

    private static ValidationException malformedOption(String optionName, String value) {
        return new ValidationException(
                String.format("Option '%s' is malformed: '%s'.", optionName, value));
    }
}
