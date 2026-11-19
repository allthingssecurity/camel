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
package org.apache.camel.utils.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.camel.processor.aggregate.cassandra.CassandraAggregationRepository;
import org.apache.camel.processor.idempotent.cassandra.CassandraIdempotentRepository;
import org.apache.camel.processor.keyvalue.cassandra.CassandraKeyValueRepository;
import org.apache.camel.support.service.ServiceSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The repositories use the CqlSession they are given, which other repositories and cql endpoints can use too: stopping
 * a repository (when its route is removed, or the CamelContext stops) must not close that session, and the repository
 * can be started again with it.
 */
class CassandraRepositoriesSessionTest {

    @Test
    void testIdempotentRepository() throws Exception {
        CqlSession session = session();
        CassandraIdempotentRepository repository = new CassandraIdempotentRepository(session);
        startStopStart(repository);
        verify(session, never()).close();
        assertSame(session, repository.getSession());
    }

    @Test
    void testAggregationRepository() throws Exception {
        CqlSession session = session();
        CassandraAggregationRepository repository = new CassandraAggregationRepository(session);
        startStopStart(repository);
        verify(session, never()).close();
        assertSame(session, repository.getSession());
    }

    @Test
    void testKeyValueRepository() throws Exception {
        CqlSession session = session();
        CassandraKeyValueRepository repository = new CassandraKeyValueRepository(session);
        startStopStart(repository);
        verify(session, never()).close();
        assertSame(session, repository.getSession());
    }

    private static void startStopStart(ServiceSupport repository) {
        repository.start();
        repository.stop();
        // started again, as when its route is added again
        repository.start();
        repository.stop();
    }

    private static CqlSession session() {
        CqlSession session = mock(CqlSession.class);
        when(session.prepare(any(SimpleStatement.class))).thenReturn(mock(PreparedStatement.class));
        return session;
    }
}
