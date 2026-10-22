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
package org.apache.camel.component.file.remote.mina.integration;

import java.nio.file.Files;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.remote.SftpRemoteFile;
import org.apache.camel.component.file.remote.mina.MinaSftpEndpoint;
import org.apache.camel.component.file.remote.mina.MinaSftpOperations;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * readLock=changed with fastExistsCheck=true checks the file itself instead of listing its directory, as with the sftp
 * and ftp components.
 */
@EnabledIf(value = "org.apache.camel.test.infra.ftp.services.embedded.SftpUtil#hasRequiredAlgorithms('src/test/resources/hostkey.pem')")
class MinaSftpChangedReadLockFastExistsCheckIT extends MinaSftpServerTestSupport {

    @Override
    protected void setupResources() throws Exception {
        service.getFtpRootDir().resolve("changed").toFile().mkdirs();
    }

    private String baseUri() {
        return "mina-sftp://localhost:" + service.getPort() + "/" + service.getFtpRootDir() + "/changed"
               + "?username=admin&password=admin&strictHostKeyChecking=no&useUserKnownHostsFile=false";
    }

    @Test
    void changedReadLockWithFastExistsCheckConsumesTheFile() throws Exception {
        assertFileIsConsumed("&fastExistsCheck=true");
    }

    @Test
    void changedReadLockWithoutFastExistsCheckConsumesTheFile() throws Exception {
        // control: lists the directory of the file
        assertFileIsConsumed("");
    }

    @Test
    void listFileListsTheFileOrNothing() throws Exception {
        Files.writeString(ftpFile("changed/one.txt"), "One");

        MinaSftpEndpoint endpoint = context.getEndpoint(baseUri(), MinaSftpEndpoint.class);
        MinaSftpOperations operations = (MinaSftpOperations) endpoint.createRemoteFileOperations();
        operations.connect(endpoint.getConfiguration(), null);
        try {
            String dir = service.getFtpRootDir() + "/changed";

            SftpRemoteFile[] files = operations.listFile(dir + "/one.txt");
            assertEquals(1, files.length);
            assertEquals("one.txt", files[0].getFilename());
            assertEquals(3, files[0].getFileLength());

            assertEquals(0, operations.listFile(dir + "/missing.txt").length);
        } finally {
            operations.disconnect();
        }
    }

    private void assertFileIsConsumed(String options) throws Exception {
        Files.writeString(ftpFile("changed/hello.txt"), "Hello World");

        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("Hello World");

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from(baseUri() + "&readLock=changed&readLockCheckInterval=100&readLockTimeout=5000"
                     + "&delete=true&initialDelay=0&delay=100" + options)
                        .to("mock:result");
            }
        });

        mock.assertIsSatisfied();
    }
}
