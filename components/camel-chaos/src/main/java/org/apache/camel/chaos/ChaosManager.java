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

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.EndpointAware;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangeExtension;
import org.apache.camel.NamedNode;
import org.apache.camel.Processor;
import org.apache.camel.ServiceStatus;
import org.apache.camel.health.HealthCheck;
import org.apache.camel.health.HealthCheckHelper;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.InterceptStrategy;
import org.apache.camel.support.EndpointHelper;
import org.apache.camel.support.EventNotifierSupport;
import org.apache.camel.support.service.ServiceSupport;
import org.apache.camel.util.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the fault rules of one CamelContext, injects the faults and writes the report.
 * <p>
 * Seams used (no change to Camel core):
 * <ul>
 * <li>{@link InterceptStrategy}: every processor that sends to an endpoint ({@link EndpointAware}, i.e. {@code to}) is
 * wrapped by a {@link ChaosProcessor}. With no active rule the wrapper only reads a volatile list.</li>
 * <li>{@link EventNotifierSupport}: exchange completed / failed / redelivery / failure handled events feed the
 * report.</li>
 * <li>{@code RouteController}: a stall rule suspends a route consumer and resumes it later.</li>
 * <li>{@link HealthCheckHelper}: health is sampled before, during and after each rule (when camel-health is
 * present).</li>
 * </ul>
 * Chaos is off by default: rules can only be added when {@code camel.chaos.enabled=true} and the Camel profile is not
 * {@code prod}.
 */
public class ChaosManager extends ServiceSupport implements CamelContextAware, InterceptStrategy {

    public static final String ENABLED = "camel.chaos.enabled";
    public static final String SEED = "camel.chaos.seed";

    private static final Logger LOG = LoggerFactory.getLogger(ChaosManager.class);

    private CamelContext camelContext;
    private long seed = 42;
    private Boolean enabledOverride;
    private final AtomicLong ruleSeq = new AtomicLong();
    // all rules ever added (for the report); active ones are also in activeRules
    private final List<ChaosRule> rules = new CopyOnWriteArrayList<>();
    private volatile List<ChaosRule> activeRules = List.of();
    private final Map<String, RouteStats> routeStats = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> timeline = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService scheduler;
    private final ChaosEventNotifier notifier = new ChaosEventNotifier();

    /**
     * Returns the manager installed on the context, or null when camel-chaos is not on the classpath.
     */
    public static ChaosManager get(CamelContext context) {
        return context.getCamelContextExtension().getContextPlugin(ChaosManager.class);
    }

    /**
     * Installs a manager on the context (must be called before routes are started, since the processors are wrapped
     * when the routes are built). Done automatically by {@link ChaosContextServicePlugin}.
     */
    public static ChaosManager install(CamelContext context) {
        ChaosManager existing = get(context);
        if (existing != null) {
            return existing;
        }
        ChaosManager manager = new ChaosManager();
        manager.setCamelContext(context);
        context.getCamelContextExtension().addContextPlugin(ChaosManager.class, manager);
        context.getCamelContextExtension().addInterceptStrategy(manager);
        context.getManagementStrategy().addEventNotifier(manager.notifier);
        try {
            context.addService(manager, true, true);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot add chaos manager", e);
        }
        return manager;
    }

    @Override
    public CamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    /**
     * Forces the enabled flag (tests). When not set, the {@code camel.chaos.enabled} property decides.
     */
    public void setEnabled(Boolean enabled) {
        this.enabledOverride = enabled;
    }

    public boolean isEnabled() {
        String profile = camelContext.getCamelContextExtension().getProfile();
        if ("prod".equalsIgnoreCase(profile)) {
            return false;
        }
        if (enabledOverride != null) {
            return enabledOverride;
        }
        return "true".equalsIgnoreCase(resolve(ENABLED));
    }

    private String resolve(String key) {
        String v = null;
        try {
            v = camelContext.getPropertiesComponent().resolveProperty(key).orElse(null);
        } catch (Exception e) {
            // ignore
        }
        if (v == null) {
            v = System.getProperty(key);
        }
        if (v == null) {
            v = System.getenv(key.toUpperCase().replace('.', '_'));
        }
        return v;
    }

    @Override
    protected void doStart() throws Exception {
        String s = resolve(SEED);
        if (s != null) {
            seed = Long.parseLong(s.trim());
        }
        scheduler = camelContext.getExecutorServiceManager().newSingleThreadScheduledExecutor(this, "ChaosManager");
        scheduler.scheduleWithFixedDelay(this::expireRules, 100, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if (scheduler != null) {
            camelContext.getExecutorServiceManager().shutdownNow(scheduler);
            scheduler = null;
        }
    }

    // ---- rules ----

    public synchronized ChaosRule addRule(ChaosRule rule) {
        if (!isEnabled()) {
            throw new IllegalStateException(
                    "Chaos is disabled. Set " + ENABLED + "=true (dev/test only; never allowed with profile prod)");
        }
        if (rule.getType() == ChaosRule.Type.STALL) {
            if (rule.getRouteId() == null || camelContext.getRoute(rule.getRouteId()) == null) {
                throw new IllegalArgumentException("Stall rule needs an existing routeId: " + rule.getRouteId());
            }
            if (rule.getDurationMs() <= 0) {
                throw new IllegalArgumentException("Stall rule needs a durationMs > 0");
            }
        } else if (rule.getPattern() == null || rule.getPattern().isBlank()) {
            throw new IllegalArgumentException("Rule needs an endpoint pattern, e.g. mock:* or http*");
        }
        if (rule.getProbability() < 0 || rule.getProbability() > 1) {
            throw new IllegalArgumentException("probability must be between 0 and 1");
        }
        long seq = ruleSeq.incrementAndGet();
        if (rule.getId() == null) {
            rule.setId("chaos-" + seq);
        }
        for (ChaosRule r : activeRules) {
            if (r.getId().equals(rule.getId())) {
                throw new IllegalArgumentException("Active rule with id already exists: " + rule.getId());
            }
        }
        rule.setHealthBefore(healthSnapshot());
        rule.activate(seq, seed, System.currentTimeMillis());
        rules.add(rule);
        List<ChaosRule> copy = new ArrayList<>(activeRules);
        copy.add(rule);
        activeRules = List.copyOf(copy);
        event("rule-added", rule.getId(), null);
        LOG.info("Chaos rule added: {}", rule.toJson().toJson());

        if (rule.getType() == ChaosRule.Type.STALL) {
            stallRoute(rule);
        }
        return rule;
    }

    public synchronized boolean removeRule(String id) {
        for (ChaosRule r : activeRules) {
            if (r.getId().equals(id)) {
                endRule(r, "removed");
                return true;
            }
        }
        return false;
    }

    public synchronized void clear() {
        for (ChaosRule r : activeRules) {
            endRule(r, "removed");
        }
    }

    /**
     * Clears all rules, statistics and the timeline.
     */
    public synchronized void reset() {
        clear();
        rules.clear();
        routeStats.clear();
        timeline.clear();
    }

    public List<ChaosRule> getRules() {
        return List.copyOf(rules);
    }

    public List<ChaosRule> getActiveRules() {
        return activeRules;
    }

    public ChaosRule getRule(String id) {
        for (ChaosRule r : rules) {
            if (r.getId().equals(id)) {
                return r;
            }
        }
        return null;
    }

    synchronized void expireRules() {
        long now = System.currentTimeMillis();
        for (ChaosRule r : activeRules) {
            if (r.isExpiredAt(now)) {
                endRule(r, r.getCalls() > 0 && r.getMatched() >= r.getCalls() ? "calls-reached" : "expired");
            }
        }
    }

    private void endRule(ChaosRule r, String reason) {
        if (!r.isActive()) {
            return;
        }
        r.end(reason);
        List<ChaosRule> copy = new ArrayList<>(activeRules);
        copy.remove(r);
        activeRules = List.copyOf(copy);
        if (r.getType() == ChaosRule.Type.STALL) {
            resumeRoute(r);
        }
        r.setHealthAfter(healthSnapshot());
        event("rule-ended", r.getId(), reason);
        LOG.info("Chaos rule {} ended ({}): injected {} of {} matched", r.getId(), reason, r.getInjected(),
                r.getMatched());
    }

    // ---- stall ----

    private void stallRoute(ChaosRule rule) {
        String routeId = rule.getRouteId();
        try {
            camelContext.getRouteController().suspendRoute(routeId);
            ServiceStatus st = camelContext.getRouteController().getRouteStatus(routeId);
            rule.roll(routeId);
            stats(routeId).faults.incrementAndGet();
            stats(routeId).markFault();
            event("route-stalled", rule.getId(), routeId + " " + st);
            rule.setHealthDuring(healthSnapshot());
        } catch (Exception e) {
            endRule(rule, "stall-failed: " + e.getMessage());
        }
    }

    private void resumeRoute(ChaosRule rule) {
        String routeId = rule.getRouteId();
        try {
            ServiceStatus st = camelContext.getRouteController().getRouteStatus(routeId);
            if (st == ServiceStatus.Suspended) {
                camelContext.getRouteController().resumeRoute(routeId);
            } else if (st != null && st.isStopped()) {
                camelContext.getRouteController().startRoute(routeId);
            }
            event("route-resumed", rule.getId(), routeId);
        } catch (Exception e) {
            event("route-resume-failed", rule.getId(), routeId + ": " + e.getMessage());
        }
    }

    // ---- injection (called by ChaosProcessor) ----

    boolean hasActiveRules() {
        return !activeRules.isEmpty();
    }

    /**
     * Applies the active rules matching this endpoint. Returns an exception to set on the exchange, or null.
     */
    Exception beforeSend(Exchange exchange, String endpointUri) {
        List<ChaosRule> list = activeRules;
        if (list.isEmpty()) {
            return null;
        }
        long now = System.currentTimeMillis();
        for (ChaosRule r : list) {
            if (r.getType() == ChaosRule.Type.STALL || !r.isActive() || r.isExpiredAt(now)) {
                continue;
            }
            if (!EndpointHelper.matchEndpoint(camelContext, endpointUri, r.getPattern())) {
                continue;
            }
            String routeId = exchange.getFromRouteId();
            if (!r.roll(routeId)) {
                continue;
            }
            if (routeId != null) {
                stats(routeId).faults.incrementAndGet();
                stats(routeId).markFault();
            }
            if (r.getHealthDuring() == null) {
                r.setHealthDuring(healthSnapshot());
            }
            if (r.getType() == ChaosRule.Type.LATENCY) {
                try {
                    Thread.sleep(r.getDelayMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else if (r.getType() == ChaosRule.Type.FAIL) {
                return createException(r, endpointUri);
            }
        }
        return null;
    }

    private Exception createException(ChaosRule r, String endpointUri) {
        String msg = r.getMessage() != null
                ? r.getMessage() : "Chaos rule " + r.getId() + " injected a failure calling " + endpointUri;
        if (r.getException() != null) {
            try {
                Class<?> type = camelContext.getClassResolver().resolveMandatoryClass(r.getException());
                Constructor<?> ctr = type.getConstructor(String.class);
                Object o = ctr.newInstance(msg);
                if (o instanceof Exception e) {
                    return e;
                }
            } catch (Exception e) {
                LOG.debug("Cannot create exception {}; using ChaosException", r.getException(), e);
            }
        }
        return new ChaosException(msg);
    }

    // ---- InterceptStrategy ----

    @Override
    public Processor wrapProcessorInInterceptors(
            CamelContext context, NamedNode definition, Processor target, Processor nextTarget) {
        if (target instanceof EndpointAware) {
            return new ChaosProcessor(this, target);
        }
        return target;
    }

    // ---- health ----

    Map<String, Object> healthSnapshot() {
        Map<String, Object> answer = new LinkedHashMap<>();
        answer.put("time", Instant.now().toString());
        Collection<HealthCheck.Result> results;
        try {
            results = HealthCheckHelper.invoke(camelContext);
        } catch (Exception e) {
            results = List.of();
        }
        if (results == null || results.isEmpty()) {
            answer.put("state", "UNKNOWN");
            answer.put("note", "no health checks (add camel-health)");
            return answer;
        }
        String overall = "UP";
        List<String> down = new ArrayList<>();
        for (HealthCheck.Result r : results) {
            if (r.getState() == HealthCheck.State.DOWN) {
                overall = "DOWN";
                down.add(r.getCheck().getId());
            } else if (r.getState() == HealthCheck.State.UNKNOWN && "UP".equals(overall)) {
                overall = "UNKNOWN";
            }
        }
        answer.put("state", overall);
        if (!down.isEmpty()) {
            answer.put("down", down);
        }
        return answer;
    }

    // ---- report ----

    /**
     * The report: rules with their counters and health samples, per-route observations and a verdict per route.
     */
    public JsonObject report() {
        expireRules();
        JsonObject root = new JsonObject();
        root.put("enabled", isEnabled());
        root.put("seed", seed);
        List<Object> rl = new ArrayList<>();
        for (ChaosRule r : rules) {
            rl.add(r.toJson());
        }
        root.put("rules", rl);

        Map<String, Object> routes = new LinkedHashMap<>();
        for (Map.Entry<String, RouteStats> e : routeStats.entrySet()) {
            routes.put(e.getKey(), e.getValue().toJson(e.getKey()));
        }
        root.put("routes", routes);
        root.put("healthNow", healthSnapshot());
        root.put("timeline", new ArrayList<>(timeline));
        return root;
    }

    /**
     * Plain text version of {@link #report()}.
     */
    public String reportText() {
        expireRules();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Chaos report (enabled=%s, seed=%d)%n", isEnabled(), seed));
        sb.append(String.format("%nRules:%n"));
        for (ChaosRule r : rules) {
            sb.append(
                    String.format("  %-10s %-7s %-20s state=%-6s matched=%d injected=%d health before=%s during=%s after=%s%n",
                            r.getId(), r.getType().name().toLowerCase(),
                            r.getPattern() != null ? r.getPattern() : "route " + r.getRouteId(),
                            r.getState(), r.getMatched(), r.getInjected(),
                            state(r.getHealthBefore()), state(r.getHealthDuring()), state(r.getHealthAfter())));
        }
        sb.append(String.format("%nRoutes:%n"));
        for (Map.Entry<String, RouteStats> e : routeStats.entrySet()) {
            JsonObject jo = e.getValue().toJson(e.getKey());
            sb.append(String.format(
                    "  %-12s completed=%s failed=%s handledByErrorHandler=%s redeliveries=%s faults=%s completedAfterLastFault=%s verdict=%s%n",
                    e.getKey(), jo.get("completed"), jo.get("failed"), jo.get("failureHandled"),
                    jo.get("redeliveries"), jo.get("faultsInjected"), jo.get("completedAfterLastFault"),
                    jo.get("verdict")));
        }
        return sb.toString();
    }

    private static Object state(Map<String, Object> h) {
        return h != null ? h.get("state") : "-";
    }

    private void event(String type, String ruleId, String detail) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("time", Instant.now().toString());
        m.put("event", type);
        if (ruleId != null) {
            m.put("rule", ruleId);
        }
        if (detail != null) {
            m.put("detail", detail);
        }
        m.put("health", healthSnapshot().get("state"));
        timeline.add(m);
    }

    private RouteStats stats(String routeId) {
        return routeStats.computeIfAbsent(routeId, k -> new RouteStats());
    }

    final class RouteStats {
        final AtomicLong completed = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final AtomicLong failureHandled = new AtomicLong();
        final AtomicLong redeliveries = new AtomicLong();
        final AtomicLong faults = new AtomicLong();
        final AtomicLong completedAfterLastFault = new AtomicLong();
        volatile long lastFaultAt;

        void markFault() {
            lastFaultAt = System.currentTimeMillis();
            completedAfterLastFault.set(0);
        }

        JsonObject toJson(String routeId) {
            JsonObject jo = new JsonObject();
            jo.put("completed", completed.get());
            jo.put("failed", failed.get());
            jo.put("failureHandled", failureHandled.get());
            jo.put("redeliveries", redeliveries.get());
            jo.put("faultsInjected", faults.get());
            jo.put("completedAfterLastFault", completedAfterLastFault.get());
            ServiceStatus st = camelContext.getRouteController().getRouteStatus(routeId);
            jo.put("routeStatus", st != null ? st.name() : "unknown");
            jo.put("verdict", verdict(routeId, st));
            return jo;
        }

        private String verdict(String routeId, ServiceStatus st) {
            boolean ruleActive = activeRules.stream()
                    .anyMatch(r -> routeId.equals(r.getRouteId()) || r.getType() != ChaosRule.Type.STALL);
            if (faults.get() == 0) {
                return "no-faults";
            }
            if (ruleActive) {
                return "chaos-active";
            }
            if (st == null || !st.isStarted()) {
                return "NOT-RECOVERED (route " + (st != null ? st.name() : "missing") + ")";
            }
            if (completedAfterLastFault.get() == 0) {
                return "NOT-RECOVERED (no exchange completed after the last fault)";
            }
            if (failed.get() > 0) {
                return "RECOVERED, but " + failed.get() + " exchange(s) failed unhandled (possible message loss)";
            }
            return "RECOVERED";
        }
    }

    final class ChaosEventNotifier extends EventNotifierSupport {

        ChaosEventNotifier() {
            setIgnoreCamelContextEvents(true);
            setIgnoreRouteEvents(true);
            setIgnoreServiceEvents(true);
            setIgnoreExchangeCreatedEvent(true);
            setIgnoreExchangeSendingEvents(true);
            setIgnoreExchangeSentEvents(true);
            setIgnoreStepEvents(true);
        }

        @Override
        public void notify(CamelEvent event) {
            if (!(event instanceof CamelEvent.ExchangeEvent ee)) {
                return;
            }
            String routeId = ee.getExchange().getFromRouteId();
            if (routeId == null) {
                return;
            }
            RouteStats s = stats(routeId);
            if (event instanceof CamelEvent.ExchangeRedeliveryEvent) {
                s.redeliveries.incrementAndGet();
            } else if (event instanceof CamelEvent.ExchangeFailureHandledEvent) {
                s.failureHandled.incrementAndGet();
            } else if (event instanceof CamelEvent.ExchangeFailedEvent) {
                s.failed.incrementAndGet();
            } else if (event instanceof CamelEvent.ExchangeCompletedEvent) {
                ExchangeExtension ext = ee.getExchange().getExchangeExtension();
                if (ext.isErrorHandlerHandledSet() && ext.isErrorHandlerHandled()) {
                    // completed because the error handler handled a failure: counted as failureHandled
                    return;
                }
                s.completed.incrementAndGet();
                if (s.lastFaultAt > 0) {
                    s.completedAfterLastFault.incrementAndGet();
                }
            }
        }
    }
}
