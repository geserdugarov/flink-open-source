/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;

/**
 * Parser factory which generates a {@link StandaloneApplicationClusterConfiguration} from a given
 * list of command line arguments.
 */
public class StandaloneApplicationClusterConfigurationParserFactory
        implements ParserResultFactory<StandaloneApplicationClusterConfiguration> {

    private static final Option JOB_CLASS_NAME_OPTION =
            Option.builder("j")
                    .longOpt("job-classname")
                    .required(false)
                    .hasArg(true)
                    .argName("job class name")
                    .desc("Class name of the job to run.")
                    .build();

    private static final Option JOB_ID_OPTION =
            Option.builder("jid")
                    .longOpt("job-id")
                    .required(false)
                    .hasArg(true)
                    .argName("job id")
                    .desc("Job ID of the job to run.")
                    .build();

    private static final Option JARS_OPTION =
            Option.builder("jars")
                    .longOpt("jars")
                    .required(false)
                    .hasArgs()
                    .valueSeparator(',')
                    .argName("jar file(s) for job")
                    .desc("Jar file of the job to run.")
                    .build();

    @Override
    public Options getOptions() {
        final Options options = new Options();
        options.addOption(CONFIG_DIR_OPTION);
        options.addOption(JARS_OPTION);
        options.addOption(JOB_CLASS_NAME_OPTION);
        options.addOption(JOB_ID_OPTION);
        options.addOption(DYNAMIC_PROPERTY_OPTION);
        options.addOption(CliFrontendParser.SAVEPOINT_PATH_OPTION);
        options.addOption(CliFrontendParser.SAVEPOINT_ALLOW_NON_RESTORED_OPTION);

        return options;
    }

    @Override
    public StandaloneApplicationClusterConfiguration createResult(@Nonnull CommandLine commandLine)
            throws FlinkParseException {
        final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
        final Properties dynamicProperties =
                commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
        final SavepointRestoreSettings savepointRestoreSettings =
                CliFrontendParser.createSavepointRestoreSettings(commandLine);
        final JobID jobId = getJobId(commandLine);
        final String jobClassName = commandLine.getOptionValue(JOB_CLASS_NAME_OPTION.getOpt());
        final String[] jarFiles = commandLine.getOptionValues(JARS_OPTION.getOpt());

        return new StandaloneApplicationClusterConfiguration(
                configDir,
                dynamicProperties,
                commandLine.getArgs(),
                savepointRestoreSettings,
                jobId,
                jobClassName,
                jarFiles);
    }

    @Nullable
    private static JobID getJobId(CommandLine commandLine) throws FlinkParseException {
        String jobId = commandLine.getOptionValue(JOB_ID_OPTION.getOpt());
        if (jobId == null) {
            return null;
        }
        try {
            return JobID.fromHexString(jobId);
        } catch (IllegalArgumentException e) {
            throw createFlinkParseException(JOB_ID_OPTION, e);
        }
    }

    private static FlinkParseException createFlinkParseException(Option option, Exception cause) {
        return new FlinkParseException(
                String.format("Failed to parse '--%s' option", option.getLongOpt()), cause);
    }
}
