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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.connectors.kafka.format.JsonFormatFactory;
import org.apache.flink.cdc.connectors.kafka.format.debezium.DebeziumJsonFormatFactory;
import org.apache.flink.connector.base.DeliveryGuarantee;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for {@link KafkaDataSinkOptions}. */
public class KafkaDataSinkOptions {

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<PartitionStrategy> PARTITION_STRATEGY =
            key("partition.strategy")
                    .enumType(PartitionStrategy.class)
                    .defaultValue(PartitionStrategy.ALL_TO_ZERO)
                    .withDescription(
                            "Defines the strategy for sending record to kafka topic, "
                                    + "available options are `all-to-zero` and `hash-by-key`, default option is `all-to-zero`.");

    public static final ConfigOption<String> KEY_FORMAT =
            key("key.format")
                    .stringType()
                    .defaultValue(JsonFormatFactory.IDENTIFIER)
                    .withDescription(
                            "Defines the format identifier for encoding key data, "
                                    + "available options are `csv` and `json`, default option is `json`.");

    public static final ConfigOption<String> VALUE_FORMAT =
            key("value.format")
                    .stringType()
                    .defaultValue(DebeziumJsonFormatFactory.IDENTIFIER)
                    .withDescription(
                            "Defines the format identifier for encoding value data, "
                                    + "available options are `debezium-json` and `canal-json`, default option is `debezium-json`.");

    public static final ConfigOption<String> TOPIC =
            key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional. If this parameter is configured, all events will be sent to this topic.");

    public static final ConfigOption<Boolean> SINK_ADD_TABLEID_TO_HEADER_ENABLED =
            key("sink.add-tableId-to-header-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional. If this parameter is configured, a header with key of 'namespace','schemaName','tableName' will be added for each Kafka record.");

    public static final ConfigOption<String> SINK_CUSTOM_HEADER =
            key("sink.custom-header")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "custom headers for each kafka record. Each header are separated by ',', separate key and value by ':'. For example, we can set headers like 'key1:value1,key2:value2'.");
}
