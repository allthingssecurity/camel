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
package org.apache.camel.component.kubernetes.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import org.apache.camel.cluster.CamelClusterView;
import org.apache.camel.cluster.CamelPreemptiveClusterService;
import org.apache.camel.component.kubernetes.KubernetesConfiguration;
import org.apache.camel.component.kubernetes.cluster.lock.impl.NativeLeaseResourceManager;
import org.apache.camel.component.kubernetes.cluster.utils.LeaderRecorder;
import org.apache.camel.component.kubernetes.cluster.utils.LeaseLockSimulator;
import org.apache.camel.component.kubernetes.cluster.utils.LockTestServer;
import org.apache.camel.support.cluster.RebalancingCamelClusterService;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two pods and two partitions. When the route of one pod for a partition stops, its view of the partition is released
 * and the pod gives up the lease, but the pod stays ready. The other pod must take the partition over, also when the
 * partitions are rebalanced: the pod that no longer takes part in the partition is not counted as a member of it.
 */
class KubernetesRebalancingPartitionMembersTest extends CamelTestSupport {

    private static final int LEASE_TIME_MILLIS = 2000;
    private static final int RENEW_DEADLINE_MILLIS = 1000;
    private static final int RETRY_PERIOD_MILLIS = 200;
    private static final String[] PODS = { "mypod-0", "mypod-1" };
    private static final String[] PARTITIONS = { "partition-0", "partition-1" };

    private final Map<String, LockTestServer<?>> lockServers = new HashMap<>();
    private final Map<String, LeaseLockSimulator> simulators = new HashMap<>();
    private final Map<String, CamelPreemptiveClusterService> services = new HashMap<>();
    private final Map<String, CamelClusterView> views = new HashMap<>();
    private final Map<String, LeaderRecorder> recorders = new HashMap<>();

    @AfterEach
    void shutdownLock() {
        for (LockTestServer<?> server : lockServers.values()) {
            try {
                server.destroy();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Test
    void partitionGivenUpByReadyPodIsTakenOverWithRebalancing() throws Exception {
        createCluster(true);
        context.start();

        // one partition each
        await().atMost(30, TimeUnit.SECONDS).until(() -> leaderOf(PARTITIONS[0]) != null
                && leaderOf(PARTITIONS[1]) != null && !leaderOf(PARTITIONS[0]).equals(leaderOf(PARTITIONS[1])));
        String partition = PARTITIONS[0];
        String owner = leaderOf(partition);
        String other = otherPod(owner);
        await().atMost(10, TimeUnit.SECONDS).until(() -> candidates(partition).size() == 2);

        // the route of the owner for the partition stops: the pod stays ready
        services.get(owner).releaseView(views.get(owner + "/" + partition));

        // the other pod takes it over (within a few retry periods), and keeps its own partition
        await().atMost(10, TimeUnit.SECONDS).until(() -> other.equals(leaderOf(partition)));
        assertEquals(other, leaderOf(PARTITIONS[1]));
        assertFalse(candidates(partition).stream().anyMatch(c -> c.startsWith(owner)),
                "The pod that gave up the partition is still a candidate: " + candidates(partition));
        assertEquals(1, views.get(other + "/" + partition).getMembers().size());
    }

    @Test
    void partitionsAreBalancedAgainWhenThePodTakesPartAgain() throws Exception {
        createCluster(true);
        context.start();

        await().atMost(30, TimeUnit.SECONDS).until(() -> leaderOf(PARTITIONS[0]) != null
                && leaderOf(PARTITIONS[1]) != null && !leaderOf(PARTITIONS[0]).equals(leaderOf(PARTITIONS[1])));
        String partition = PARTITIONS[0];
        String owner = leaderOf(partition);
        String other = otherPod(owner);
        services.get(owner).releaseView(views.get(owner + "/" + partition));
        await().atMost(10, TimeUnit.SECONDS).until(() -> other.equals(leaderOf(partition))
                && other.equals(leaderOf(PARTITIONS[1])));

        // the route of the owner starts again: the partitions are balanced again, one each
        CamelClusterView view = services.get(owner).getView(partition);
        assertNotNull(view);
        await().atMost(30, TimeUnit.SECONDS).until(() -> leaderOf(PARTITIONS[0]) != null
                && leaderOf(PARTITIONS[1]) != null && !leaderOf(PARTITIONS[0]).equals(leaderOf(PARTITIONS[1])));
    }

    @Test
    void partitionGivenUpByReadyPodIsTakenOverWithoutRebalancing() throws Exception {
        createCluster(false);
        context.start();

        await().atMost(30, TimeUnit.SECONDS).until(() -> leaderOf(PARTITIONS[0]) != null && leaderOf(PARTITIONS[1]) != null);
        String partition = PARTITIONS[0];
        String owner = leaderOf(partition);
        String other = otherPod(owner);

        services.get(owner).releaseView(views.get(owner + "/" + partition));

        await().atMost(10, TimeUnit.SECONDS).until(() -> other.equals(leaderOf(partition)));
        assertNotEquals(owner, leaderOf(partition));
    }

    private void createCluster(boolean rebalancing) throws Exception {
        for (String pod : PODS) {
            lockServers.put(pod, new LockTestServer<>());
        }
        for (String partition : PARTITIONS) {
            simulators.put(partition, new LeaseLockSimulator("leaders-" + partition));
        }
        for (String pod : PODS) {
            KubernetesConfiguration configuration = new KubernetesConfiguration();
            configuration.setKubernetesClient(lockServers.get(pod).createClient());
            KubernetesClusterService service = new KubernetesClusterService(configuration);
            service.setNamespace("test");
            service.setPodName(pod);
            service.setLeaseDurationMillis(LEASE_TIME_MILLIS);
            service.setRenewDeadlineMillis(RENEW_DEADLINE_MILLIS);
            service.setRetryPeriodMillis(RETRY_PERIOD_MILLIS);
            service.setJitterFactor(1.1);
            service.setLeaseResourceType(LeaseResourceType.Lease);
            CamelPreemptiveClusterService member
                    = rebalancing ? new RebalancingCamelClusterService(service, RETRY_PERIOD_MILLIS) : service;
            context().addService(member);
            services.put(pod, member);
            for (String partition : PARTITIONS) {
                LeaderRecorder recorder = new LeaderRecorder();
                CamelClusterView view = member.getView(partition);
                view.addEventListener(recorder);
                views.put(pod + "/" + partition, view);
                recorders.put(pod + "/" + partition, recorder);
            }
        }
        for (LockTestServer<?> server : lockServers.values()) {
            for (String pod : PODS) {
                server.addPod(pod);
            }
            for (LeaseLockSimulator simulator : simulators.values()) {
                server.addSimulator(simulator);
            }
        }
    }

    private String leaderOf(String partition) {
        for (String pod : PODS) {
            if (isLocalLeader(pod, partition)) {
                return pod;
            }
        }
        return null;
    }

    private boolean isLocalLeader(String pod, String partition) {
        CamelClusterView view = views.get(pod + "/" + partition);
        return view.getLocalMember().isLeader() && pod.equals(recorders.get(pod + "/" + partition).getCurrentLeader());
    }

    private static String otherPod(String pod) {
        assertTrue(pod.equals(PODS[0]) || pod.equals(PODS[1]));
        return pod.equals(PODS[0]) ? PODS[1] : PODS[0];
    }

    private Set<String> candidates(String partition) {
        Lease lease = simulators.get(partition).getResource();
        Set<String> candidates = new NativeLeaseResourceManager().decodeCandidates(lease, partition);
        return candidates != null ? candidates : Set.of();
    }
}
