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

import java.util.Arrays;

final class ToolsTagsHelper {
    private ToolsTagsHelper() {
    }

    /**
     * Splits a comma-separated list of tags or names. Each part is trimmed and empty parts are dropped, as the tags of
     * an ai-tool endpoint are.
     *
     * @param  tagList the comma-separated list
     * @return         the trimmed, non-empty parts
     */
    public static String[] splitTags(String tagList) {
        return Arrays.stream(tagList.split(","))
                .map(String::trim)
                .filter(tag -> !tag.isEmpty())
                .toArray(String[]::new);
    }
}
