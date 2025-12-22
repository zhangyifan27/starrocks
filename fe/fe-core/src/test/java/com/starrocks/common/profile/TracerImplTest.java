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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class TracerImplTest {
    private TracerImpl tracer;
    private Stopwatch timing;
    private TimeWatcher watcher;
    private VarTracer varTracer;
    private CommandLogTracer logTracer;
    private CommandLogTracer reasonTracer;

    @Before
    public void setUp() {
        timing = Stopwatch.createStarted();
        watcher = new TimeWatcher();
        varTracer = new VarTracer();
        logTracer = new CommandLogTracer();
        reasonTracer = new CommandLogTracer();
        tracer = new TracerImpl(timing, watcher, varTracer, logTracer, reasonTracer);
    }

    @Test
    public void testWatchScope() throws InterruptedException {
        // Test basic scope watching
        Timer timer = tracer.watchScope("TestScope");
        Assert.assertNotNull(timer);
        Assert.assertEquals("TestScope", timer.name());
        
        Thread.sleep(10);
        timer.close();
        
        Assert.assertTrue(timer.getTotalTime() >= 10);
    }

    @Test
    public void testNestedScopes() throws InterruptedException {
        // Test nested scopes
        try (Timer outer = tracer.watchScope("OuterScope")) {
            Thread.sleep(10);
            try (Timer inner = tracer.watchScope("InnerScope")) {
                Thread.sleep(10);
            }
        }
        
        Optional<Timer> outerTimer = tracer.getSpecifiedTimer("OuterScope");
        Optional<Timer> innerTimer = tracer.getSpecifiedTimer("InnerScope");
        
        Assert.assertTrue(outerTimer.isPresent());
        Assert.assertTrue(innerTimer.isPresent());
        Assert.assertTrue(outerTimer.get().getTotalTime() >= innerTimer.get().getTotalTime());
    }

    @Test
    public void testLog() {
        // Test simple log
        tracer.log("Test event");
        
        String logs = tracer.printLogs();
        Assert.assertTrue(logs.contains("Test event"));
    }

    @Test
    public void testLogWithArgs() {
        // Test log with arguments (SLF4J style placeholders)
        tracer.log("Test event with args: {}, {}", "value", 123);
        
        String logs = tracer.printLogs();
        Assert.assertTrue(logs.contains("Test event with args: value, 123"));
    }

    @Test
    public void testLazyLog() {
        // Test lazy log with function
        tracer.log(args -> String.format("Lazy log: %s", args[0]), "test");
        
        String logs = tracer.printLogs();
        Assert.assertTrue(logs.contains("Lazy log: test"));
    }

    @Test
    public void testReason() {
        // Test reason logging (SLF4J style placeholders)
        tracer.reason("Test reason: {}", "explanation");
        
        String reasons = tracer.printReasons();
        Assert.assertTrue(reasons.contains("Test reason: explanation"));
    }

    @Test
    public void testRecord() {
        // Test recording variables
        tracer.record("TestVar", "TestValue");
        
        List<Var<?>> vars = tracer.getAllVars();
        Assert.assertEquals(1, vars.size());
        Assert.assertEquals("TestVar", vars.get(0).getName());
        Assert.assertEquals("TestValue", vars.get(0).getValue());
    }

    @Test
    public void testCount() {
        // Test counting
        tracer.count("TestCounter", 10);
        tracer.count("TestCounter", 20);
        
        List<Var<?>> vars = tracer.getAllVars();
        Assert.assertEquals(1, vars.size());
        Assert.assertEquals("TestCounter", vars.get(0).getName());
        Assert.assertEquals(30L, vars.get(0).getValue());
    }

    @Test
    public void testPrintScopeTimer() throws InterruptedException {
        // Test printing scope timers
        try (Timer timer = tracer.watchScope("TestScope")) {
            Thread.sleep(10);
        }
        
        String output = tracer.printScopeTimer();
        Assert.assertTrue(output.contains("TestScope"));
        Assert.assertTrue(output.contains("Tracer Cost:"));
    }

    @Test
    public void testPrintTiming() throws InterruptedException {
        // Test printing all timing information
        try (Timer timer = tracer.watchScope("TestScope")) {
            Thread.sleep(10);
        }
        Thread.sleep(5); // Add delay to avoid timestamp collision
        tracer.log("Test log");
        Thread.sleep(5); // Add delay to avoid timestamp collision
        tracer.record("TestVar", "TestValue");
        
        String output = tracer.printTiming();
        Assert.assertTrue("Output should contain 'TestScope'", output.contains("TestScope"));
        Assert.assertTrue("Output should contain 'Test log', but got: " + output, output.contains("Test log"));
        Assert.assertTrue("Output should contain 'TestVar'", output.contains("TestVar"));
    }

    @Test
    public void testPrintVars() {
        // Test printing variables
        tracer.record("Var1", "Value1");
        tracer.record("Var2", "Value2");
        tracer.count("Counter", 100);
        
        String output = tracer.printVars();
        Assert.assertTrue(output.contains("Var1"));
        Assert.assertTrue(output.contains("Value1"));
        Assert.assertTrue(output.contains("Var2"));
        Assert.assertTrue(output.contains("Value2"));
        Assert.assertTrue(output.contains("Counter"));
        Assert.assertTrue(output.contains("100"));
    }

    @Test
    public void testPrintLogs() {
        // Test printing logs
        tracer.log("Log 1");
        tracer.log("Log 2");
        
        String output = tracer.printLogs();
        Assert.assertTrue(output.contains("Log 1"));
        Assert.assertTrue(output.contains("Log 2"));
    }

    @Test
    public void testPrintReasons() {
        // Test printing reasons
        tracer.reason("Reason 1");
        tracer.reason("Reason 2");
        
        String output = tracer.printReasons();
        Assert.assertTrue(output.contains("Reason 1"));
        Assert.assertTrue(output.contains("Reason 2"));
    }

    @Test
    public void testGetSpecifiedTimer() throws InterruptedException {
        // Test getting specific timer
        try (Timer timer = tracer.watchScope("SpecificTimer")) {
            Thread.sleep(10);
        }
        
        Optional<Timer> found = tracer.getSpecifiedTimer("SpecificTimer");
        Assert.assertTrue(found.isPresent());
        Assert.assertEquals("SpecificTimer", found.get().name());
        Assert.assertTrue(found.get().getTotalTime() >= 10);
    }

    @Test
    public void testGetSpecifiedTimerNotFound() {
        // Test getting non-existent timer
        Optional<Timer> found = tracer.getSpecifiedTimer("NonExistent");
        Assert.assertFalse(found.isPresent());
    }

    @Test
    public void testToRuntimeProfile() throws InterruptedException {
        // Test converting to runtime profile
        try (Timer timer = tracer.watchScope("TestScope")) {
            Thread.sleep(10);
        }
        tracer.record("TestVar", "TestValue");
        tracer.reason("Test reason");
        
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        tracer.toRuntimeProfile(profile);
        
        String profileStr = profile.toString();
        Assert.assertTrue(profileStr.contains("TestScope"));
        Assert.assertTrue(profileStr.contains("TestVar"));
        Assert.assertTrue(profileStr.contains("TestValue"));
        Assert.assertTrue(profileStr.contains("Reason"));
    }

    @Test
    public void testMultipleTimerCalls() throws InterruptedException {
        // Test multiple calls to the same scope
        try (Timer timer1 = tracer.watchScope("RepeatedScope")) {
            Thread.sleep(10);
        }
        
        try (Timer timer2 = tracer.watchScope("RepeatedScope")) {
            Thread.sleep(10);
        }
        
        Optional<Timer> timer = tracer.getSpecifiedTimer("RepeatedScope");
        Assert.assertTrue(timer.isPresent());
        // Should accumulate time from both calls
        Assert.assertTrue(timer.get().getTotalTime() >= 20);
    }

    @Test
    public void testHierarchicalVars() {
        // Test hierarchical variable names
        tracer.record("parent.child1", "value1");
        tracer.record("parent.child2", "value2");
        
        String output = tracer.printVars();
        Assert.assertTrue(output.contains("child1"));
        Assert.assertTrue(output.contains("value1"));
        Assert.assertTrue(output.contains("child2"));
        Assert.assertTrue(output.contains("value2"));
    }

    @Test
    public void testTimingOrder() throws InterruptedException {
        // Test that events are recorded in order
        tracer.log("Event 1");
        Thread.sleep(5);
        tracer.log("Event 2");
        Thread.sleep(5);
        tracer.log("Event 3");
        
        String output = tracer.printTiming();
        int idx1 = output.indexOf("Event 1");
        int idx2 = output.indexOf("Event 2");
        int idx3 = output.indexOf("Event 3");
        
        Assert.assertTrue(idx1 < idx2);
        Assert.assertTrue(idx2 < idx3);
    }

    @Test
    public void testEmptyTracer() {
        // Test tracer with no operations
        Assert.assertTrue(tracer.getAllVars().isEmpty());
        Assert.assertFalse(tracer.getSpecifiedTimer("NonExistent").isPresent());
        
        String scopeTimer = tracer.printScopeTimer();
        Assert.assertTrue(scopeTimer.contains("Tracer Cost:"));
        
        String timing = tracer.printTiming();
        Assert.assertTrue(timing.contains("Tracer Cost:"));
    }

    @Test
    public void testTracerCost() throws InterruptedException {
        // Test that tracer cost is tracked
        for (int i = 0; i < 100; i++) {
            tracer.log("Event " + i);
            tracer.record("Var" + i, "Value" + i);
        }
        
        String output = tracer.printScopeTimer();
        Assert.assertTrue(output.contains("Tracer Cost:"));
        Assert.assertTrue(output.contains("us")); // microseconds
    }

    @Test
    public void testGetLeafTimersSortedByTime() throws InterruptedException {
        // Create nested scopes: OuterScope -> InnerScope1, InnerScope2
        try (Timer outer = tracer.watchScope("OuterScope")) {
            Thread.sleep(5);
            try (Timer inner1 = tracer.watchScope("InnerScope1")) {
                Thread.sleep(30); // Longer sleep for inner1
            }
            try (Timer inner2 = tracer.watchScope("InnerScope2")) {
                Thread.sleep(10); // Shorter sleep for inner2
            }
        }

        List<Timer> leafTimers = tracer.getLeafTimersSortedByTime();

        // Should only contain leaf timers (InnerScope1, InnerScope2), not OuterScope
        Assert.assertEquals(2, leafTimers.size());

        // Should be sorted by time descending (InnerScope1 first since it took longer)
        Assert.assertEquals("InnerScope1", leafTimers.get(0).name());
        Assert.assertEquals("InnerScope2", leafTimers.get(1).name());
        Assert.assertTrue(leafTimers.get(0).getTotalTime() >= leafTimers.get(1).getTotalTime());
    }

    @Test
    public void testGetLeafTimersSortedByTimeWithSingleScope() throws InterruptedException {
        // Single scope should be considered a leaf
        try (Timer timer = tracer.watchScope("SingleScope")) {
            Thread.sleep(10);
        }

        List<Timer> leafTimers = tracer.getLeafTimersSortedByTime();
        Assert.assertEquals(1, leafTimers.size());
        Assert.assertEquals("SingleScope", leafTimers.get(0).name());
    }

    @Test
    public void testGetLeafTimersSortedByTimeEmpty() {
        // No scopes should return empty list
        List<Timer> leafTimers = tracer.getLeafTimersSortedByTime();
        Assert.assertTrue(leafTimers.isEmpty());
    }

    @Test
    public void testGetTopSlowLeafOperations() throws InterruptedException {
        // Create nested scopes with different timings
        try (Timer outer = tracer.watchScope("OuterScope")) {
            try (Timer inner1 = tracer.watchScope("SlowOperation")) {
                Thread.sleep(30);
            }
            try (Timer inner2 = tracer.watchScope("MediumOperation")) {
                Thread.sleep(20);
            }
            try (Timer inner3 = tracer.watchScope("FastOperation")) {
                Thread.sleep(10);
            }
        }

        String output = tracer.getTopSlowLeafOperations(2);

        // Should contain header
        Assert.assertTrue(output.contains("Top 2 slowest leaf operations:"));

        // Should contain top 2 slowest operations
        Assert.assertTrue(output.contains("SlowOperation"));
        Assert.assertTrue(output.contains("MediumOperation"));

        // Should NOT contain FastOperation (only top 2)
        Assert.assertFalse(output.contains("FastOperation"));

        // Should NOT contain OuterScope (it's not a leaf)
        Assert.assertFalse(output.contains("OuterScope"));

        // Should contain count info
        Assert.assertTrue(output.contains("count:"));
    }

    @Test
    public void testGetTopSlowLeafOperationsFormat() throws InterruptedException {
        // Test output format: no newlines, comma separated
        try (Timer outer = tracer.watchScope("Outer")) {
            try (Timer inner1 = tracer.watchScope("Op1")) {
                Thread.sleep(10);
            }
            try (Timer inner2 = tracer.watchScope("Op2")) {
                Thread.sleep(10);
            }
        }

        String output = tracer.getTopSlowLeafOperations(3);

        // Should not contain newlines
        Assert.assertFalse(output.contains("\n"));

        // Should contain comma separator between entries
        Assert.assertTrue(output.contains(", "));
    }

    @Test
    public void testGetTopSlowLeafOperationsWithLargerTopN() throws InterruptedException {
        // Request more items than available
        try (Timer timer = tracer.watchScope("OnlyScope")) {
            Thread.sleep(10);
        }

        String output = tracer.getTopSlowLeafOperations(10);

        // Should show actual count (1), not requested count (10)
        Assert.assertTrue(output.contains("Top 1 slowest leaf operations:"));
        Assert.assertTrue(output.contains("OnlyScope"));
    }

    @Test
    public void testGetTopSlowLeafOperationsEmpty() {
        // No scopes
        String output = tracer.getTopSlowLeafOperations(5);
        Assert.assertTrue(output.contains("Top 0 slowest leaf operations:"));
    }
}
