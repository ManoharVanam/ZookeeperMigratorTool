/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.toolkit.zkmigrator;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

public class ZooKeeperMigratorMain {

    enum Mode {READ, WRITE}

    private static final String JAVA_HOME = "JAVA_HOME";
    private static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME";
    private static final String HEADER = System.lineSeparator() + "A tool for importing and exporting data from ZooKeeper." + System.lineSeparator() + System.lineSeparator();
    private static final String FOOTER = System.lineSeparator() + "Java home: " +
            System.getenv(JAVA_HOME) + System.lineSeparator() + "NiFi Toolkit home: " + System.getenv(NIFI_TOOLKIT_HOME);

    private static final Option OPTION_ZK_MIGRATOR_HELP = Option.builder("h")
            .longOpt("help")
            .desc("display help/usage info")
            .build();
    private static final Option OPTION_ZK_ENDPOINT = Option.builder("z")
            .longOpt("zookeeper")
            .desc("ZooKeeper endpoint string (ex. host:port)")
            .hasArg()
            .argName("zookeeper-endpoint")
            .required()
            .build();
    private static final Option OPTION_ZK_PATHS = Option.builder("p")
            .longOpt("paths")
            .desc("Zookeeper paths (ex: /kafka,/brokers)")
            .hasArg()
            .argName("multiple paths")
            .required()
            .build();
    private static final Option OPTION_RECEIVE = Option.builder("r")
            .longOpt("receive")
            .desc("receives data from zookeeper and writes to the given filename or standard output")
            .build();
    private static final Option OPTION_SEND = Option.builder("s")
            .longOpt("send")
            .desc("sends data to zookeeper read from the given filename or standard input")
            .build();
    private static final Option OPTION_ZK_AUTH_INFO = Option.builder("a")
            .longOpt("auth")
            .desc("username and password for the given ZooKeeper path")
            .hasArg()
            .argName("username:password")
            .build();
    private static final Option OPTION_ZK_KRB_CONF_FILE = Option.builder("k")
            .longOpt("krb-conf")
            .desc("JAAS file containing Kerberos config")
            .hasArg()
            .argName("jaas-filename")
            .numberOfArgs(1)
            .build();
//    private static final Option OPTION_FILES = Option.builder("f")
//            .longOpt("file")
//            .desc("file to be used for ZooKeeper data")
//            .hasArg()
//            .argName("filename")
//            .build();
    private static final Option OPTION_DIR = Option.builder("d")
            .longOpt("directory")
            .desc("directory to be used for ZooKeeper data")
            .hasArg()
            .argName("directory")
            .build();

    private static final Option OPTION_IGNORE_SOURCE = Option.builder("i")
            .longOpt("ignore-source")
            .desc("ignores the source ZooKeeper endpoint specified in the exported data")
            .build();
    private static final Option OPTION_USE_EXISTING_ACL = Option.builder()
            .longOpt("use-existing-acl")
            .desc("allow write of existing acl data to destination")
            .build();

    private static Options createOptions() {
        final Options options = new Options();
        options.addOption(OPTION_ZK_MIGRATOR_HELP);
        options.addOption(OPTION_ZK_ENDPOINT);
        options.addOption(OPTION_ZK_PATHS);
        options.addOption(OPTION_ZK_AUTH_INFO);
//        options.addOption(OPTION_FILES);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_IGNORE_SOURCE);
        options.addOption(OPTION_USE_EXISTING_ACL);
        final OptionGroup optionGroupAuth = new OptionGroup().addOption(OPTION_ZK_AUTH_INFO).addOption(OPTION_ZK_KRB_CONF_FILE);
        optionGroupAuth.setRequired(false);
        options.addOptionGroup(optionGroupAuth);
        final OptionGroup optionGroupReadWrite = new OptionGroup().addOption(OPTION_RECEIVE).addOption(OPTION_SEND);
        optionGroupReadWrite.setRequired(true);
        options.addOptionGroup(optionGroupReadWrite);

        return options;
    }

    private static void printUsage(String errorMessage, Options options) {
        Preconditions.checkNotNull(options, "command line options were not specified");
        if (errorMessage != null) {
            System.out.println(errorMessage + System.lineSeparator());
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.setDescPadding(0);
        helpFormatter.printHelp(ZooKeeperMigratorMain.class.getCanonicalName(), HEADER, options, FOOTER, true);
    }

    public static void main(String[] args) throws IOException {
        PrintStream output = System.out;
        System.setOut(System.err);

        final Options options = createOptions();
        final CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(options, args);
            if (commandLine.hasOption(OPTION_ZK_MIGRATOR_HELP.getLongOpt())) {
                printUsage(null, options);
            } else {
                final String zookeeperUri = commandLine.getOptionValue(OPTION_ZK_ENDPOINT.getOpt());
                final String paths = commandLine.getOptionValue(OPTION_ZK_PATHS.getOpt());
                final Mode mode = commandLine.hasOption(OPTION_RECEIVE.getOpt()) ? Mode.READ : Mode.WRITE;
//                final String filename = commandLine.getOptionValue(OPTION_FILES.getOpt());
                final String dirName = commandLine.getOptionValue(OPTION_DIR.getOpt());
                final String auth = commandLine.getOptionValue(OPTION_ZK_AUTH_INFO.getOpt());
                final String jaasFilename = commandLine.getOptionValue(OPTION_ZK_KRB_CONF_FILE.getOpt());
                final boolean ignoreSource = commandLine.hasOption(OPTION_IGNORE_SOURCE.getLongOpt());
                final boolean useExistingACL = commandLine.hasOption(OPTION_USE_EXISTING_ACL.getLongOpt());
                final ZooKeeperMigrator.AuthMode authMode;
                final byte[] authData;
                if (auth != null) {
                    authMode = ZooKeeperMigrator.AuthMode.DIGEST;
                    authData = auth.getBytes(StandardCharsets.UTF_8);
                } else {
                    authData = null;
                    if (!Strings.isNullOrEmpty(jaasFilename)) {
                        authMode = ZooKeeperMigrator.AuthMode.SASL;
                        System.setProperty("java.security.auth.login.config", jaasFilename);
                    } else {
                        authMode = ZooKeeperMigrator.AuthMode.OPEN;
                    }
                }

//                final ZooKeeperMigrator zookeeperMigrator = new ZooKeeperMigrator(zookeeperUri);
                if (mode.equals(Mode.READ)) {
                    // reading multiple paths and storing in output dir with path name as file name.
                    ZooKeeperMigrator zookeeperMigrator = new ZooKeeperMigrator(zookeeperUri);
                    String[] pathArray = paths.split(",");
                    for(int i = 0 ; i < pathArray.length ; i++) {
                        String file = getFileName(dirName, pathArray[i]);
                        try (OutputStream zkData = file != null  ? new FileOutputStream(Paths.get(file).toFile()) : output) {
                            zookeeperMigrator.readZooKeeper(zkData, authMode, authData, pathArray[i]);
                        }
                    }

                } else {
                    // read multiple files and write into one zookeeper path
                    if(paths.split(",").length > 1){
                        throw new IOException("Single zookeeper path is allowed.");
                    }
                    ZooKeeperMigrator zookeeperMigrator = new ZooKeeperMigrator(zookeeperUri+paths);
                    File[] files = Paths.get(dirName).toFile().listFiles();
                    for(File file : files) {

                        try (InputStream zkData = file != null ? new FileInputStream(file) : System.in) {
                            zookeeperMigrator.writeZooKeeper(zkData, authMode, authData, ignoreSource, useExistingACL);
                        }
                    }
                }
            }
        } catch (ParseException e) {
            printUsage(e.getLocalizedMessage(), options);
        } catch (IOException | KeeperException | InterruptedException | ExecutionException e) {
            throw new IOException(String.format("unable to perform operation: %s", e.getLocalizedMessage()), e);
        }
    }

    private static String getFileName(String filename, String path) {
        path = path.replaceAll("/","_");
        return filename + "zk" + path + "_data.json";

    }
}
