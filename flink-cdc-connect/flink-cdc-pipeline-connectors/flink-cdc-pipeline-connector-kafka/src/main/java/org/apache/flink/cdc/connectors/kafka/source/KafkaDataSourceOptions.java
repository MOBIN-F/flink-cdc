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

import org.apache.flink.cdc.common.configuration.ConfigOption;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for the Kafka pipeline source. */
public class KafkaDataSourceOptions {

    public static final String PROPERTIES_PREFIX = "properties.";

    public static final ConfigOption<String> TOPIC =
            key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Comma-separated Kafka topics to consume.");

    public static final ConfigOption<String> TOPIC_PATTERN =
            key("topic-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Regular expression matching Kafka topics to consume.");

    public static final ConfigOption<String> GROUP_ID =
            key("group-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka consumer group id.");

    public static final ConfigOption<String> PRIMARY_KEYS =
            key("primary-keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated primary key columns used for all tables when no table-specific mapping is configured.");

    public static final ConfigOption<String> PRIMARY_KEYS_MAPPING =
            key("primary-keys.mapping")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table-specific primary keys in 'database.table:key1,key2;database.table:key' format.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            key("scan.startup.mode")
                    .stringType()
                    .defaultValue("group-offsets")
                    .withDescription(
                            "Startup mode. Supported values are earliest-offset, latest-offset, and group-offsets.");

    private KafkaDataSourceOptions() {}
}
