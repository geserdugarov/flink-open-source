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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.util.LinkedList;
import java.util.List;

/** Non rich test output format which stores everything in a list. */
public class TestNonRichOutputFormat implements OutputFormat<String> {
    public final List<String> output = new LinkedList<>();

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(InitializationContext context) {}

    @Override
    public void close() {}

    @Override
    public void writeRecord(String record) {
        output.add(record);
    }

    public void clear() {
        output.clear();
    }
}
