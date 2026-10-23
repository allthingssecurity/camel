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
package org.apache.camel.component.mina;

import java.io.ByteArrayInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.converter.stream.InputStreamCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * With textline=true the reply of a consumer is a line of text, whatever the type of the body the route ends with.
 */
class MinaTextlineReplyBodyTest extends BaseMinaTest {

    // not valid UTF-8, so a conversion to a String would change it
    private static final byte[] BINARY = { (byte) 0xC3, 0x28, 0x00, (byte) 0xFF };

    private final int streamPort = getNextPort();
    private final int latin1Port = getNextPort();
    private final int streamCachePort = getNextPort();
    private final int udpPort = getNextPort();

    @Test
    void byteArrayReplyIsSentAsText() {
        String reply = template.requestBody("mina:tcp://localhost:" + getPort() + "?textline=true&sync=true", "ping",
                String.class);
        assertEquals("pong", reply);
    }

    @Test
    void streamReplyIsSentAsText() {
        String reply = template.requestBody("mina:tcp://localhost:" + streamPort + "?textline=true&sync=true", "ping",
                String.class);
        assertEquals("pong from a stream", reply);
    }

    @Test
    void streamCacheReplyIsSentAsText() {
        String reply = template.requestBody("mina:tcp://localhost:" + streamCachePort + "?textline=true&sync=true", "ping",
                String.class);
        assertEquals("pong from a stream cache", reply);
    }

    @Test
    void byteArrayReplyIsDecodedWithTheEncodingOfTheEndpoint() {
        String reply = template.requestBody(
                "mina:tcp://localhost:" + latin1Port + "?textline=true&sync=true&encoding=ISO-8859-1", "ping", String.class);
        assertEquals("café", reply);
    }

    @Test
    void datagramReplyIsNotConvertedAsTheTextlineCodecIsNotUsed() throws Exception {
        // datagrams always use their own codec, which writes the bytes of a byte[] as they are
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(10000);
            InetAddress address = InetAddress.getByName("127.0.0.1");
            byte[] ping = "ping".getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(ping, ping.length, address, udpPort));

            byte[] buf = new byte[128];
            DatagramPacket reply = new DatagramPacket(buf, buf.length);
            socket.receive(reply);
            assertArrayEquals(BINARY, Arrays.copyOf(reply.getData(), reply.getLength()));
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                fromF("mina:tcp://localhost:%d?textline=true&sync=true", getPort())
                        .setBody(constant("pong".getBytes(StandardCharsets.UTF_8)));

                fromF("mina:tcp://localhost:%d?textline=true&sync=true", streamPort)
                        .setBody(exchange -> new ByteArrayInputStream("pong from a stream".getBytes(StandardCharsets.UTF_8)));

                fromF("mina:tcp://localhost:%d?textline=true&sync=true", streamCachePort)
                        .setBody(exchange -> new InputStreamCache("pong from a stream cache".getBytes(StandardCharsets.UTF_8)));

                fromF("mina:tcp://localhost:%d?textline=true&sync=true&encoding=ISO-8859-1", latin1Port)
                        .setBody(constant("café".getBytes(StandardCharsets.ISO_8859_1)));

                fromF("mina:udp://127.0.0.1:%d?textline=true&sync=true", udpPort)
                        .setBody(constant(BINARY));
            }
        };
    }
}
