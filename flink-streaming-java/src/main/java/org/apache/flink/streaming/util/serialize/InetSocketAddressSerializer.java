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

package org.apache.flink.streaming.util.serialize;

import org.apache.flink.annotation.PublicEvolving;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.net.InetSocketAddress;

/**
 * InetSocketAddressSerializer is a Kryo 5.x serializer for InetSocketAddress.
 *
 * <p>With the Flink Kryo 2.x code, the Chill library was used for a Kryo 2.x
 * InetSocketAddressSerializer.
 *
 * <p>All other serializers from the Chill library are have analogs in the Kryo 5 base library. This
 * is the one exception that isn't in the Kryo 5 library and needs a manual port.
 */
@PublicEvolving
public class InetSocketAddressSerializer extends Serializer<InetSocketAddress> {
    @Override
    public void write(Kryo kryo, Output output, InetSocketAddress object) {
        output.writeString(object.getHostName());
        output.writeInt(object.getPort(), true);
    }

    @Override
    public InetSocketAddress read(Kryo kryo, Input input, Class<? extends InetSocketAddress> type) {
        String host = input.readString();
        int port = input.readInt(true);
        return new InetSocketAddress(host, port);
    }
}
