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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.fail;

/** Test proper handling of IPv6 address literals in URLs. */
@SuppressWarnings("serial")
public class IPv6HostnamesITCase extends TestLogger {

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private Configuration getConfiguration() {
        final Inet6Address ipv6address = getLocalIPv6Address();
        if (ipv6address == null) {
            throw new AssumptionViolatedException(
                    "--- Cannot find a non-loopback local IPv6 address that Pekko/Netty can bind to; skipping IPv6HostnamesITCase");
        }
        final String addressString = ipv6address.getHostAddress();
        log.info("Test will use IPv6 address " + addressString + " for connection tests");

        Configuration config = new Configuration();
        config.set(JobManagerOptions.ADDRESS, addressString);
        config.set(TaskManagerOptions.HOST, addressString);
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
        return config;
    }

    @Test
    public void testClusterWithIPv6host() {
        try {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);

            // get input data
            DataStream<String> text = env.fromData(WordCountData.TEXT.split("\n"));

            DataStream<Tuple2<String, Integer>> counts =
                    text.flatMap(
                                    new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                        @Override
                                        public void flatMap(
                                                String value,
                                                Collector<Tuple2<String, Integer>> out)
                                                throws Exception {
                                            for (String token : value.toLowerCase().split("\\W+")) {
                                                if (token.length() > 0) {
                                                    out.collect(
                                                            new Tuple2<String, Integer>(token, 1));
                                                }
                                            }
                                        }
                                    })
                            .keyBy(x -> x.f0)
                            .window(GlobalWindows.createWithEndOfStreamTrigger())
                            .reduce(
                                    new ReduceFunction<Tuple2<String, Integer>>() {
                                        @Override
                                        public Tuple2<String, Integer> reduce(
                                                Tuple2<String, Integer> value1,
                                                Tuple2<String, Integer> value2)
                                                throws Exception {
                                            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                                        }
                                    });

            List<Tuple2<String, Integer>> result =
                    CollectionUtil.iteratorToList(counts.executeAndCollect());

            TestBaseUtils.compareResultAsText(result, WordCountData.COUNTS_AS_TUPLES);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private Inet6Address getLocalIPv6Address() {
        try {
            Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface netInterface = e.nextElement();

                // for each address of the network interface
                Enumeration<InetAddress> ee = netInterface.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress addr = ee.nextElement();

                    if (addr instanceof Inet6Address
                            && (!addr.isLoopbackAddress())
                            && (!addr.isAnyLocalAddress())) {
                        // see if it is possible to bind to the address
                        InetSocketAddress socketAddress = new InetSocketAddress(addr, 0);

                        try {
                            log.info("Considering address " + addr);

                            // test whether we can bind a socket to that address
                            log.info("Testing whether sockets can bind to " + addr);
                            ServerSocket sock = new ServerSocket();
                            sock.bind(socketAddress);
                            sock.close();

                            // test whether Pekko's netty can bind to the address
                            log.info("Testing whether Pekko can use " + addr);
                            final RpcService rpcService =
                                    RpcSystem.load()
                                            // this port is only used for advertising (==no port
                                            // conflicts) since we explicitly provide a bind port
                                            .remoteServiceBuilder(new Configuration(), null, "8081")
                                            .withBindAddress(addr.getHostAddress())
                                            .withBindPort(0)
                                            .createAndStart();
                            rpcService.closeAsync().get();

                            log.info("Using address " + addr);
                            return (Inet6Address) addr;
                        } catch (IOException ignored) {
                            // fall through the loop
                        }
                    }
                }
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
