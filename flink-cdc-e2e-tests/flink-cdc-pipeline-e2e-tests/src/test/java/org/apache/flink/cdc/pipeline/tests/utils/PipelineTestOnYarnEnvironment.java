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

package org.apache.flink.cdc.pipeline.tests.utils;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Test environment running pipeline job on YARN mini-cluster. */
public class PipelineTestOnYarnEnvironment extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineTestOnYarnEnvironment.class);

    protected static final YarnConfiguration YARN_CONFIGURATION;
    private YarnClient yarnClient = null;
    protected static MiniYARNCluster yarnCluster = null;
    protected org.apache.flink.configuration.Configuration flinkConfiguration;

    protected static final String TEST_CLUSTER_NAME_KEY = "flink-yarn-minicluster-name";
    protected static final int NUM_NODEMANAGERS = 2;

    protected static File yarnSiteXML = null;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    // copy from org.apache.flink.yarn.YarnTestBase
    static {
        YARN_CONFIGURATION = new YarnConfiguration();
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32);
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
                4096); // 4096 is the available memory anyways
        YARN_CONFIGURATION.setBoolean(
                YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
        YARN_CONFIGURATION.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
        YARN_CONFIGURATION.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
        // so we have to change the number of cores for testing.
        YARN_CONFIGURATION.setFloat(
                YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0F);
        YARN_CONFIGURATION.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, getYarnClasspath());
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1000);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 5000);
        YARN_CONFIGURATION.set(TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-application");
    }
    // 54627
    @Before
    public void before() throws Exception {
        if (yarnClient == null) {
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(getYarnConfiguration());
            yarnClient.start();
        }
    }

    protected static YarnConfiguration getYarnConfiguration() {
        return YARN_CONFIGURATION;
    }

    public static void startMiniYARNCluster() {
        try {
            LOG.info("Starting up MiniYARNCluster");
            if (yarnCluster == null) {
                final String testName =
                        YARN_CONFIGURATION.get(PipelineTestOnYarnEnvironment.TEST_CLUSTER_NAME_KEY);
                yarnCluster =
                        new MiniYARNCluster(
                                testName == null ? "YarnTest_" + UUID.randomUUID() : testName,
                                NUM_NODEMANAGERS,
                                1,
                                1);

                yarnCluster.init(YARN_CONFIGURATION);
                yarnCluster.start();
            }

            File targetTestClassesFolder = new File("target/test-classes");
            writeYarnSiteConfigXML(YARN_CONFIGURATION, targetTestClassesFolder);

            Map<String, String> map = new HashMap<String, String>(System.getenv());
            map.put(
                    "IN_TESTS",
                    "yes we are in tests"); // see YarnClusterDescriptor() for more infos
            map.put("YARN_CONF_DIR", targetTestClassesFolder.getAbsolutePath());
            map.put("MAX_LOG_FILE_NUMBER", "10");
            CommonTestUtils.setEnv(map);

            assertThat(yarnCluster.getServiceState()).isEqualTo(Service.STATE.STARTED);

            // wait for the nodeManagers to connect
            while (!yarnCluster.waitForNodeManagersToConnect(500)) {
                LOG.info("Waiting for Nodemanagers to connect");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.error("setup failure", ex);
            fail("");
        }
    }

    // write yarn-site.xml to target/test-classes so that flink pick can pick up this when
    // initializing YarnClient properly from classpath
    public static void writeYarnSiteConfigXML(Configuration yarnConf, File targetFolder)
            throws IOException {
        yarnSiteXML = new File(targetFolder, "/yarn-site.xml");
        try (FileWriter writer = new FileWriter(yarnSiteXML)) {
            yarnConf.writeXml(writer);
            writer.flush();
        }
    }

    @After
    public void after() {
        yarnClient.stop();
    }

    public void submitPipelineJob(String pipelineJob, Path... jars)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        Map<String, String> env = getEnv();
        processBuilder.environment().putAll(getEnv());
        Path yamlScript = temporaryFolder.newFile("mysql-to-values.yml").toPath();
        Files.write(yamlScript, pipelineJob.getBytes());

        List<String> commandList = new ArrayList<>();



        commandList.add(env.get("FLINK_CDC_HOME") + "/bin/flink-cdc.sh");
        commandList.add("-t");
        commandList.add("yarn-application");
        commandList.add(yamlScript.toAbsolutePath().toString());
        for (Path jar : jars) {
            commandList.add("--jar");
            commandList.add(jar.toString());
        }

        processBuilder.command(commandList);
//        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
//        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
//        processBuilder.redirectErrorStream(true);
        LOG.info("starting flink-cdc task with flink on yarn-application");
        Process process = processBuilder.start();
        process.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        StringBuilder output = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            System.out.println(line + "======");
            output.append(line).append("\n");
        }
        LOG.info("started flink-cdc task with flink on yarn-application");
    }

    public Map<String, String> getEnv() {
        Path flinkHome = Paths.get("target/flink-1.18.1");
        Map<String, String> env = new HashMap<>();
        env.put("FLINK_HOME", flinkHome.toString());
        env.put("FLINK_CONF_DIR", flinkHome.resolve("conf").toString());
        Path flinkcdcHome =
                Paths.get(
                        "../../flink-cdc-dist/target/flink-cdc-3.3-SNAPSHOT-bin/flink-cdc-3.3-SNAPSHOT");
        env.put("FLINK_CDC_HOME", flinkcdcHome.toString());
        env.put("HADOOP_CLASSPATH", getYarnClasspath());
        return env;
    }

    @Test
    public void test() throws IOException {
        getEnv();
    }

    /**
     * Searches for the yarn.classpath file generated by the "dependency:build-classpath" maven
     * plugin in "flink-yarn-tests".
     *
     * @return a classpath suitable for running all YARN-launched JVMs
     */
    private static String getYarnClasspath() {
        final String start = "../flink-cdc-pipeline-e2e-tests";
        try {
            File classPathFile = findFile(start, (dir, name) -> name.equals("yarn.classpath"));
            return FileUtils.readFileToString(
                    classPathFile, StandardCharsets.UTF_8); // potential NPE is supposed to be fatal
        } catch (Throwable t) {
            LOG.error(
                    "Error while getting YARN classpath in {}",
                    new File(start).getAbsoluteFile(),
                    t);
            throw new RuntimeException("Error while getting YARN classpath", t);
        }
    }

    /** Locate a file or directory. */
    public static File findFile(String startAt, FilenameFilter fnf) {
        File root = new File(startAt);
        String[] files = root.list();
        if (files == null) {
            return null;
        }
        for (String file : files) {
            File f = new File(startAt + File.separator + file);
            if (f.isDirectory()) {
                File r = findFile(f.getAbsolutePath(), fnf);
                if (r != null) {
                    return r;
                }
            } else if (fnf.accept(f.getParentFile(), f.getName())) {
                return f;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        startMiniYARNCluster();
    }

    @Test
    public void t() throws IOException, InterruptedException {
        submitPipelineJob("bbb");
    }
}
