#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Resolves the classpaths of the demo application (app.cp) and of Camel CLI (cli.cp).
# Run it after installing camel-chaos, camel-cli-connector and camel-jbang-core (see DEMO.md).
cd "$(dirname "$0")"
MVNW="$(cd ../../.. && pwd)/mvnw"
mkdir -p .mvn cli/.mvn
"$MVNW" -B -q -ntp -f pom.xml dependency:build-classpath -Dmdep.outputFile="$PWD/app.cp"
"$MVNW" -B -q -ntp -f cli/pom.xml dependency:build-classpath -Dmdep.outputFile="$PWD/cli.cp"
echo "app.cp and cli.cp written"
