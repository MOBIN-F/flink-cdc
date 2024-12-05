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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaUtil;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for mysql cdc to Kafka pipeline job. */
@RunWith(Parameterized.class)
public class MysqlToKafkaE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlToKafkaE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    protected static final long EVENT_WAITING_TIMEOUT = 60000L;

    private static AdminClient admin;
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private TableId table;
    private String topic;

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeClass
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();

        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
        LOG.info("Containers are started.");
    }

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        LOG.info("create test topic");
        createTestTopic(1, TOPIC_REPLICATION_FACTOR);
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "  schema-info.enabled: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: kafka\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  topic: %s\n"
                                + "  sink.schema-info-enable: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        topic,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, kafkaCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        Thread.sleep(60000);
        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, false, 0);

        assertThat(deserializeValues(collectedRecords))
                .isEqualTo(getExpectedRecords("expectedEvents/kafka-debezium-json.txt"));

        //        waitUntilSpecificEvent(
        //                String.format(
        //                        "DataChangeEvent{tableId=%s.customers, before=[], after=[104,
        // user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
        //                        mysqlInventoryDatabase.getDatabaseName()));
        //        waitUntilSpecificEvent(
        //                String.format(
        //                        "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare
        // tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}",
        //                        mysqlInventoryDatabase.getDatabaseName()));
        //
        //        validateResult(getExpectedRecords("expectedEvents/kafka-debezium-json.txt"));

        //        LOG.info("Begin incremental reading stage.");
        //        // generate binlogs
        //        String mysqlJdbcUrl =
        //                String.format(
        //                        "jdbc:mysql://%s:%s/%s",
        //                        MYSQL.getHost(),
        //                        MYSQL.getDatabasePort(),
        //                        mysqlInventoryDatabase.getDatabaseName());
        //        try (Connection conn =
        //                     DriverManager.getConnection(
        //                             mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
        //             Statement stat = conn.createStatement()) {
        //            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE
        // id=106;");
        //            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");
        //
        //            // Perform DDL changes after the binlog is generated
        //            waitUntilSpecificEvent(
        //                    String.format(
        //                            "DataChangeEvent{tableId=%s.products, before=[106, hammer,
        // 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter
        // hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
        //                            mysqlInventoryDatabase.getDatabaseName()));
        //
        //            // modify table schema
        //            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
        //            stat.execute(
        //                    "INSERT INTO products VALUES (default,'jacket','water resistent white
        // wind breaker',0.2, null, null, null, 1);"); // 110
        //            stat.execute(
        //                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter
        // ',5.18, null, null, null, 1);"); // 111
        //            stat.execute(
        //                    "UPDATE products SET description='new water resistent white wind
        // breaker', weight='0.5' WHERE id=110;");
        //            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
        //            stat.execute("DELETE FROM products WHERE id=111;");
        //        } catch (SQLException e) {
        //            LOG.error("Update table for CDC failed.", e);
        //            throw e;
        //        }
        //
        //        waitUntilSpecificEvent(
        //                String.format(
        //                        "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big
        // 2-wheel scooter , 5.17, null, null, null, 1], after=[], op=DELETE, meta=()}",
        //                        mysqlInventoryDatabase.getDatabaseName()));

        //        validateResult(
        //                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz
        // carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer,
        // 1.0, null, null, null], op=UPDATE, meta=()}",
        //                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted
        // rocks, 5.3, null, null, null], after=[107, rocks, box of assorted rocks, 5.1, null, null,
        // null], op=UPDATE, meta=()}",
        //                "AddColumnEvent{tableId=%s.products,
        // addedColumns=[ColumnWithPosition{column=`new_col` INT, position=LAST,
        // existedColumnName=null}]}",
        //                "DataChangeEvent{tableId=%s.products, before=[], after=[110, jacket, water
        // resistent white wind breaker, 0.2, null, null, null, 1], op=INSERT, meta=()}",
        //                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big
        // 2-wheel scooter , 5.18, null, null, null, 1], op=INSERT, meta=()}",
        //                "DataChangeEvent{tableId=%s.products, before=[110, jacket, water resistent
        // white wind breaker, 0.2, null, null, null, 1], after=[110, jacket, new water resistent
        // white wind breaker, 0.5, null, null, null, 1], op=UPDATE, meta=()}",
        //                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel
        // scooter , 5.18, null, null, null, 1], after=[111, scooter, Big 2-wheel scooter , 5.17,
        // null, null, null, 1], op=UPDATE, meta=()}",
        //                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel
        // scooter , 5.17, null, null, null, 1], after=[], op=DELETE, meta=()}");
    }

    private void validateResult(List<String> expectedEvents) throws Exception {
        String dbName = mysqlInventoryDatabase.getDatabaseName();
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(String.format(event, dbName, dbName));
        }
    }

    private void waitUntilSpecificEvent(String event) throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + MysqlToKafkaE2eITCase.EVENT_WAITING_TIMEOUT;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = taskManagerConsumer.toUtf8String();
            if (stdout.contains(event + "\n")) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get specific event: "
                            + event
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, boolean committed, int... partitionArr) {
        Properties properties = getKafkaClientConfiguration();
        Set<Integer> partitions = new HashSet<>();
        for (int partition : partitionArr) {
            partitions.add(partition);
        }
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties, committed, partitions);
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
        return standardProps;
    }

    private void createTestTopic(int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        table =
                TableId.tableId(
                        "default_namespace", "default_schema", UUID.randomUUID().toString());
        topic = table.toString();
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }

    private List<String> deserializeValues(List<ConsumerRecord<byte[], byte[]>> records)
            throws IOException {
        List<String> result = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            result.add(new String(record.value(), "UTF-8"));
        }
        return result;
    }

    protected List<String> getExpectedRecords(String resourceDirFormat) throws Exception {
        URL url =
                MysqlToKafkaE2eITCase.class
                        .getClassLoader()
                        .getResource(String.format(resourceDirFormat));
        return Files.readAllLines(Paths.get(url.toURI())).stream()
                .filter(this::isRecordLine)
                .map(line -> String.format(line, mysqlInventoryDatabase.getDatabaseName()))
                .collect(Collectors.toList());
    }

    protected boolean isRecordLine(String line) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.readTree(line);
            return !StringUtils.isEmpty(line);
        } catch (JsonProcessingException e) {
            return false;
        }
    }
}
