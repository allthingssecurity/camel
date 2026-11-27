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
package org.apache.camel.component.langchain4j.agent;

import org.apache.camel.CamelContext;
import org.apache.camel.spi.OAuthClientAuthenticationFactory;
import org.apache.camel.spi.OAuthClientConfig;

/**
 * The OAuth client factory of the tests, in the place of camel-oauth (see
 * META-INF/services/org/apache/camel/oauth-client-authentication-factory): a test decides which token every profile
 * gives.
 */
public class TestOAuthClientAuthenticationFactory implements OAuthClientAuthenticationFactory {

    private static volatile String token;

    /**
     * Sets the token that every profile gives.
     */
    static void token(String token) {
        TestOAuthClientAuthenticationFactory.token = token;
    }

    /**
     * No profile gives a token.
     */
    static void reset() {
        token = null;
    }

    @Override
    public String resolveToken(OAuthClientConfig config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String resolveToken(CamelContext context, String profileName) {
        String answer = token;
        if (answer == null) {
            throw new IllegalStateException("No OAuth token for the profile " + profileName);
        }
        return answer;
    }

    @Override
    public String resolveToken(CamelContext context) {
        throw new UnsupportedOperationException();
    }
}
