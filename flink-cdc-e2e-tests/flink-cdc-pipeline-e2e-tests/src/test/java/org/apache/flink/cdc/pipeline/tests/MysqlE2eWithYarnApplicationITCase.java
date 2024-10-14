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

import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestOnYarnEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/** End-to-end tests for mysql cdc pipeline job. */
// @RunWith(Parameterized.class)
public class MysqlE2eWithYarnApplicationITCase extends PipelineTestOnYarnEnvironment {
    private static final Logger LOG =
            LoggerFactory.getLogger(MysqlE2eWithYarnApplicationITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    protected static final long EVENT_WAITING_TIMEOUT = 60000L;

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
                            .withNetwork(Network.newNetwork())
                            .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
        startMiniYARNCluster();
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    @Disabled
    public void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: %S\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        1);
        //        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        //        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        //        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob);
        //        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        Thread.sleep(300000);
        //        Thread.sleep(100000);
        //        File file = findFile("../flink-cdc-pipeline-e2e-tests", (dir, name) ->
        // name.equals("taskmanager.out"));
        //        List<String> lines = Files.readAllLines(file.toPath());
        //        lines.forEach(System.out::println);

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

        validateResult(
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}");
    }

    private void validateResult(String... expectedEvents) throws Exception {
        String dbName = mysqlInventoryDatabase.getDatabaseName();
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(String.format(event, dbName, dbName));
        }
    }

    private void waitUntilSpecificEvent(String event) throws Exception {
        //                boolean result = false;
        //                long endTimeout = System.currentTimeMillis() +
        //         MysqlE2eWithYarnApplicationITCase.EVENT_WAITING_TIMEOUT;
        //                while (System.currentTimeMillis() < endTimeout) {
        //                    String stdout = taskManagerConsumer.toUtf8String();
        //                    if (stdout.contains(event + "\n")) {
        //                        result = true;
        //                        break;
        //                    }
        //                    Thread.sleep(1000);
        //                }
        //                if (!result) {
        //                    throw new TimeoutException(
        //                            "failed to get specific event: "
        //                                    + event
        //                                    + " from stdout: "
        //                                    + taskManagerConsumer.toUtf8String());
        //                }
    }

    //    @Test
    //    public void log() {
    //        File file = findFile("../flink-cdc-pipeline-e2e-tests", (dir, name) ->
    // name.equals("taskmanager.out"));
    //        System.out.println(file.toPath());
    //    }
}