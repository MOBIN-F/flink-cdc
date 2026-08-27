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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaUtil;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksContainer;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** End-to-end tests for a multi-partition Kafka Debezium JSON to StarRocks pipeline. */
class KafkaToStarRocksE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToStarRocksE2eITCase.class);
    private static final String DATABASE = "inventory";
    private static final String KAFKA_ALIAS = "kafka";
    private static final String STARROCKS_ALIAS = "starrocks";

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(KAFKA_ALIAS);

    @Container
    private static final StarRocksContainer STARROCKS_CONTAINER =
            new StarRocksContainer(NETWORK).withNetworkAliases(STARROCKS_ALIAS);

    private AdminClient admin;
    private KafkaProducer<byte[], byte[]> producer;
    private String topic;

    @BeforeAll
    static void startExternalSystems() throws Exception {
        Startables.deepStart(Stream.of(KAFKA_CONTAINER, STARROCKS_CONTAINER)).join();
        STARROCKS_CONTAINER.waitForLog(
                ".*Enjoy the journey to StarRocks blazing-fast lake-house engine!.*\\s", 1, 240);
        waitForStarRocksBackend();
    }

    @BeforeEach
    void setUpKafka() throws Exception {
        topic = "debezium-customers-" + UUID.randomUUID();
        Properties properties = kafkaProperties();
        admin = AdminClient.create(properties);
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, (short) 1)))
                .all()
                .get();
        properties.setProperty("key.serializer", ByteArraySerializer.class.getName());
        properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @AfterEach
    void tearDownKafka() {
        producer.close();
        admin.deleteTopics(Collections.singletonList(topic));
        admin.close();
    }

    @Test
    void testNewSchemaThenHistoricalSchemaFromAnotherPartition() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: kafka\n"
                                + "  topic: %s\n"
                                + "  group-id: %s\n"
                                + "  scan.startup.mode: earliest-offset\n"
                                + "  properties.bootstrap.servers: %s:9092\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: starrocks\n"
                                + "  jdbc-url: jdbc:mysql://%s:9030\n"
                                + "  load-url: %s:8080\n"
                                + "  username: root\n"
                                + "  password: \"\"\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 2\n"
                                + "  schema.change.behavior: lenient\n",
                        topic, UUID.randomUUID(), KAFKA_ALIAS, STARROCKS_ALIAS, STARROCKS_ALIAS);
        Path kafkaJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        Path starRocksJar = TestUtils.getResource("starrocks-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaJar, starRocksJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        send(
                1,
                key("int64", 2147483648L),
                value(
                        newFields(),
                        "{\"id\":2147483648,\"name\":\"new\",\"email\":\"new@example.com\"}"));
        waitForRows(1);

        send(0, key("int32", 2), value(oldFields(), "{\"id\":2,\"name\":\"old\"}"));
        waitForRows(2);

        try (Connection connection = STARROCKS_CONTAINER.createConnection(DATABASE);
                Statement statement = connection.createStatement()) {
            try (ResultSet rows =
                    statement.executeQuery("SELECT id, name, email FROM customers ORDER BY id")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(2L);
                assertThat(rows.getString(2)).isEqualTo("old");
                assertThat(rows.getString(3)).isNull();
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(2147483648L);
                assertThat(rows.getString(3)).isEqualTo("new@example.com");
                assertThat(rows.next()).isFalse();
            }
            try (ResultSet columns =
                    statement.executeQuery(
                            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
                                    + "FROM information_schema.COLUMNS "
                                    + "WHERE TABLE_SCHEMA='inventory' AND TABLE_NAME='customers'")) {
                boolean bigintId = false;
                boolean widenedName = false;
                boolean email = false;
                while (columns.next()) {
                    String name = columns.getString(1);
                    if ("id".equals(name)) {
                        bigintId = "bigint".equalsIgnoreCase(columns.getString(2));
                    } else if ("name".equals(name)) {
                        widenedName = columns.getLong(3) >= 128L;
                    } else if ("email".equals(name)) {
                        email = true;
                    }
                }
                assertThat(bigintId).isTrue();
                assertThat(widenedName).isTrue();
                assertThat(email).isTrue();
            }
        }
    }

    private void send(int partition, byte[] key, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        producer.flush();
    }

    private void waitForRows(int expected) throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = STARROCKS_CONTAINER.createConnection(DATABASE);
                    Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery("SELECT COUNT(*) FROM customers")) {
                if (resultSet.next() && resultSet.getInt(1) == expected) {
                    return;
                }
            } catch (Exception e) {
                LOG.info("StarRocks table is not ready yet.", e);
            }
            Thread.sleep(1000L);
        }
        fail("Timed out waiting for {} rows in StarRocks.", expected);
    }

    private static void waitForStarRocksBackend() throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(4).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = STARROCKS_CONTAINER.createConnection("");
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SHOW BACKENDS")) {
                if (resultSet.next() && resultSet.getBoolean("Alive")) {
                    return;
                }
            } catch (Exception e) {
                LOG.info("StarRocks backend is not ready yet.", e);
            }
            Thread.sleep(1000L);
        }
        throw new IllegalStateException("StarRocks backend startup timed out.");
    }

    private Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        return properties;
    }

    private static byte[] key(String type, long id) {
        return bytes(
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\""
                        + type
                        + "\",\"optional\":false,\"field\":\"id\"}]},\"payload\":{\"id\":"
                        + id
                        + "}}");
    }

    private static byte[] value(String fields, String row) {
        String rowSchema =
                "{\"type\":\"struct\",\"fields\":["
                        + fields
                        + "],\"optional\":true,\"name\":\"inventory.customers.Value\"}";
        return bytes(
                "{\"schema\":{\"type\":\"struct\",\"fields\":["
                        + withField(rowSchema, "before")
                        + ","
                        + withField(rowSchema, "after")
                        + "]},\"payload\":{\"before\":null,\"after\":"
                        + row
                        + ",\"source\":{\"db\":\"inventory\",\"table\":\"customers\"},\"op\":\"c\"}}");
    }

    private static String oldFields() {
        return "{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},"
                + stringField("name", 32);
    }

    private static String newFields() {
        return "{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},"
                + stringField("name", 128)
                + ","
                + stringField("email", 64);
    }

    private static String stringField(String name, int length) {
        return "{\"type\":\"string\",\"optional\":true,\"parameters\":{"
                + "\"__debezium.source.column.length\":\""
                + length
                + "\"},\"field\":\""
                + name
                + "\"}";
    }

    private static String withField(String schema, String field) {
        return schema.substring(0, schema.length() - 1) + ",\"field\":\"" + field + "\"}";
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
