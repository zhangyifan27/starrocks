// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common.profile;

import com.google.common.base.Stopwatch;
import com.starrocks.common.util.RuntimeProfile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

class TracerImpl extends Tracer {
    private final Stopwatch tracerCost = Stopwatch.createUnstarted();
    private final Stopwatch timing;
    private final TimeWatcher watcher;
    private final VarTracer varTracer;
    private final LogTracer logTracer;
    private final LogTracer reasonTracer;

    public TracerImpl(Stopwatch timing, TimeWatcher watcher, VarTracer vars, LogTracer logTracer,
                      LogTracer reasonTracer) {
        this.timing = timing;
        this.watcher = watcher;
        this.varTracer = vars;
        this.logTracer = logTracer;
        this.reasonTracer = reasonTracer;
    }

    private long timePoint() {
        return timing.elapsed(TimeUnit.MILLISECONDS);
    }

    public Timer watchScope(String name) {
        tracerCost.start();
        Timer t = watcher.scope(timePoint(), name);
        tracerCost.stop();
        return t;
    }

    public void log(String event) {
        tracerCost.start();
        logTracer.log(timePoint(), event);
        tracerCost.stop();
    }

    public void log(String event, Object... args) {
        tracerCost.start();
        logTracer.log(timePoint(), event, args);
        tracerCost.stop();
    }

    @Override
    // lazy log, use it if you want to avoid construct log string when log is disabled
    public void log(Function<Object[], String> func, Object... args) {
        tracerCost.start();
        logTracer.log(timePoint(), func, args);
        tracerCost.stop();
    }

    @Override
    public void reason(String reason, Object... args) {
        tracerCost.start();
        reasonTracer.log(timePoint(), reason, args);
        tracerCost.stop();
    }

    public void record(String name, String value) {
        tracerCost.start();
        varTracer.record(timePoint(), name, value);
        tracerCost.stop();
    }

    public void count(String name, long count) {
        tracerCost.start();
        varTracer.count(timePoint(), name, count);
        tracerCost.stop();
    }

    public List<Var<?>> getAllVars() {
        return varTracer.getAllVars();
    }

    public String printScopeTimer() {
        StringBuilder sb = new StringBuilder();
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        for (Timer timer : watcher.getAllTimerWithOrder()) {
            sb.append(String.format(fixedString, timer.getFirstTimePoint()));
            sb.append(timer);
            sb.append("\n");
        }

        printCosts(sb);
        return sb.toString();
    }

    private void printCosts(StringBuilder sb) {
        sb.append("Tracer Cost: ");
        sb.append(tracerCost.elapsed(TimeUnit.MICROSECONDS));
        sb.append("us");
    }

    public String printTiming() {
        Map<Long, String> timings = new TreeMap<>();
        for (Timer timer : watcher.getAllTimerWithOrder()) {
            timings.put(timer.getFirstTimePoint(), "watchScope: " + timer.name());
        }
        for (LogTracer.LogEvent log : logTracer.getLogs()) {
            timings.put(log.getTimePoint(), "log: " + log.getLog());
        }
        for (Var<?> var : varTracer.getAllVars()) {
            timings.put(var.getTimePoint(), "record: " + var.getName());
        }
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        StringBuilder sb = new StringBuilder();
        timings.forEach((k, v) -> {
            sb.append(String.format(fixedString, k));
            sb.append(" ");
            sb.append(v);
            sb.append("\n");
        });

        printCosts(sb);
        return sb.toString();
    }

    public String printVars() {
        StringBuilder sb = new StringBuilder();
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        for (Var<?> var : varTracer.getAllVarsWithOrder()) {
            sb.append(String.format(fixedString, var.getTimePoint()));
            sb.append(" ");
            sb.append(var);
            sb.append("\n");
        }

        printCosts(sb);
        return sb.toString();
    }

    public String printLogs() {
        StringBuilder sb = new StringBuilder();
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        for (LogTracer.LogEvent log : logTracer.getLogs()) {
            sb.append(String.format(fixedString, log.getTimePoint()));
            sb.append("    ");
            sb.append(log.getLog());
            sb.append("\n");
        }

        printCosts(sb);
        return sb.toString();
    }

    public String printReasons() {
        StringBuilder sb = new StringBuilder();
        for (LogTracer.LogEvent log : reasonTracer.getLogs()) {
            sb.append("    ");
            sb.append(log.getLog());
            sb.append("\n");
        }
        return sb.toString();
    }

    // ----------------- runtime profile -----------------
    private RuntimeProfile getRuntimeProfile(RuntimeProfile parent, Map<String, RuntimeProfile> cache,
                                             String prefix) {
        if (cache.containsKey(prefix)) {
            return cache.get(prefix);
        }
        String[] ss = prefix.split("\\.");
        StringBuilder sb = new StringBuilder();
        RuntimeProfile p = parent;
        for (String s : ss) {
            sb.append(s);
            sb.append('.');
            String tmp = sb.toString();
            if (!cache.containsKey(tmp)) {
                RuntimeProfile sp = new RuntimeProfile(s);
                p.addChild(sp);
                cache.put(tmp, sp);
            }
            p = cache.get(tmp);
        }
        return p;
    }

    private static String getKeyPrefix(String key) {
        String prefix = "";
        int index = key.lastIndexOf('.');
        if (index != -1) {
            prefix = key.substring(0, index + 1);
        }
        return prefix;
    }

    private void buildTimers(RuntimeProfile parent) {
        for (Timer timer : watcher.getAllTimerWithOrder()) {
            parent.addInfoString(timer.toString(), "");
        }
    }

    private void buildVars(RuntimeProfile parent) {
        Map<String, RuntimeProfile> profilers = new HashMap<>();
        profilers.put("", parent);
        for (Var<?> var : varTracer.getAllVarsWithOrder()) {
            String prefix = getKeyPrefix(var.name);
            String name = var.name.substring(prefix.length());
            RuntimeProfile p = getRuntimeProfile(parent, profilers, prefix);
            p.addInfoString(name, var.value.toString());
        }
    }

    private void buildReasons(RuntimeProfile profile) {
        RuntimeProfile reasons = new RuntimeProfile("Reason");
        profile.addChild(reasons);
        for (LogTracer.LogEvent log : reasonTracer.getLogs()) {
            reasons.addInfoString(log.getLog(), "");
        }
    }

    public void toRuntimeProfile(RuntimeProfile parent) {
        buildTimers(parent);
        buildVars(parent);
        buildReasons(parent);
    }

    @Override
    public Optional<Timer> getSpecifiedTimer(String name) {
        return watcher.getTimer(name);
    }

    @Override
    public String getTopSlowLeafOperations(int topN) {
        List<Timer> leafTimers = getLeafTimersSortedByTime();
        StringBuilder sb = new StringBuilder();
        sb.append("Top ").append(Math.min(topN, leafTimers.size())).append(" slowest leaf operations: ");
        int rank = 1;
        List<Timer> topTimers = leafTimers.stream().limit(topN).collect(Collectors.toList());
        for (int i = 0; i < topTimers.size(); i++) {
            Timer timer = topTimers.get(i);
            sb.append(String.format("%d. %s: %dms (count: %d)",
                    rank++, timer.name(), timer.getTotalTime(), timer.getCount()));
            if (i < topTimers.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    @Override
    public List<Timer> getLeafTimersSortedByTime() {
        List<Timer> allTimers = watcher.getAllTimerWithOrder();
        List<Timer> leafTimers = new ArrayList<>();

        // Identify leaf timers: a timer is a leaf if the next timer has a smaller or equal scope level
        for (int i = 0; i < allTimers.size(); i++) {
            Timer current = allTimers.get(i);
            boolean isLeaf = true;

            // Check if next timer has a higher scope level (meaning current has children)
            if (i + 1 < allTimers.size()) {
                Timer next = allTimers.get(i + 1);
                if (next.getScopeLevel() > current.getScopeLevel()) {
                    isLeaf = false;
                }
            }

            if (isLeaf) {
                leafTimers.add(current);
            }
        }

        return leafTimers.stream()
                .sorted(Comparator.comparingLong(Timer::getTotalTime).reversed())
                .collect(Collectors.toList());
    }
}
