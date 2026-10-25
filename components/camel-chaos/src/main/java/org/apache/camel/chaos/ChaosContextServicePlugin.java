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
package org.apache.camel.chaos;

import org.apache.camel.CamelContext;
import org.apache.camel.spi.ContextServicePlugin;

/**
 * Installs the {@link ChaosManager} when camel-chaos is on the classpath. The manager does nothing until chaos is
 * enabled ({@code camel.chaos.enabled=true}, never with profile prod) and a rule is added.
 */
public class ChaosContextServicePlugin implements ContextServicePlugin {

    @Override
    public void load(CamelContext camelContext) {
        ChaosManager.install(camelContext);
    }
}
