/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;

/**
 * Interface for operator factories which create the sink operator containing an {@link
 * OutputFormat}.
 *
 * @param <IN> The input type of the operator.
 */
@Internal
public interface OutputFormatOperatorFactory<IN, OUT> extends StreamOperatorFactory<OUT> {

    /**
     * @return output format of the operator created by this factory.
     */
    OutputFormat<IN> getOutputFormat();
}
