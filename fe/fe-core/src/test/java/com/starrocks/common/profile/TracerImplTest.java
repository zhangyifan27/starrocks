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
    public void testGetSlowOperationsSummary() throws InterruptedException {
        // Test getting slow operations summary
        try (Timer fastTimer = tracer.watchScope("FastOperation")) {
            Thread.sleep(10);
        }
        
        try (Timer slowTimer = tracer.watchScope("SlowOperation")) {
            Thread.sleep(1100); // More than 1000ms threshold
        }
        
        String summary = tracer.getSlowOperationsSummary();
        Assert.assertFalse(summary.contains("FastOperation"));
        Assert.assertTrue(summary.contains("SlowOperation"));
    }

    @Test
    public void testGetSlowOperationsSummaryEmpty() throws InterruptedException {
        // Test when no slow operations (threshold is 1000ms)
        try (Timer timer = tracer.watchScope("FastOperation")) {
            Thread.sleep(10); // Only 10ms, well below 1000ms threshold
        }
        
        String summary = tracer.getSlowOperationsSummary();
        // Should be empty since no operations exceed 1000ms
        Assert.assertTrue("Summary should be empty when no slow operations", summary.isEmpty());
    }

    @Test
    public void testGetSlowOperationsSummaryWithSlowOps() throws InterruptedException {
        // Test with operations exceeding 1000ms threshold
        try (Timer timer = tracer.watchScope("SlowOperation1")) {
            Thread.sleep(1100); // Exceeds 1000ms threshold
        }
        Thread.sleep(5); // Avoid timestamp collision
        try (Timer timer = tracer.watchScope("FastOperation")) {
            Thread.sleep(10); // Below threshold
        }
        Thread.sleep(5);
        try (Timer timer = tracer.watchScope("SlowOperation2")) {
            Thread.sleep(1050); // Exceeds 1000ms threshold
        }
        
        String summary = tracer.getSlowOperationsSummary();
        // Should contain slow operations but not fast ones
        Assert.assertTrue("Summary should contain SlowOperation1", summary.contains("SlowOperation1"));
        Assert.assertTrue("Summary should contain SlowOperation2", summary.contains("SlowOperation2"));
        Assert.assertFalse("Summary should not contain FastOperation", summary.contains("FastOperation"));
        
        // Verify format: should contain operation count and timing
        Assert.assertTrue("Summary should contain operation count [1]", summary.contains("[1]"));
    }

    @Test
    public void testGetSlowOperationsSummaryMultipleSlowOps() throws InterruptedException {
        // Test with multiple slow operations
        try (Timer timer = tracer.watchScope("SlowOp1")) {
            Thread.sleep(1100);
        }
        Thread.sleep(5);
        try (Timer timer = tracer.watchScope("SlowOp2")) {
            Thread.sleep(1200);
        }
        Thread.sleep(5);
        try (Timer timer = tracer.watchScope("SlowOp3")) {
            Thread.sleep(1050);
        }
        
        String summary = tracer.getSlowOperationsSummary();
        // Should contain all three slow operations
        Assert.assertTrue("Summary should contain SlowOp1", summary.contains("SlowOp1"));
        Assert.assertTrue("Summary should contain SlowOp2", summary.contains("SlowOp2"));
        Assert.assertTrue("Summary should contain SlowOp3", summary.contains("SlowOp3"));
        
        // Verify each operation is on a separate line
        String[] lines = summary.split("\n");
        int slowOpCount = 0;
        for (String line : lines) {
            if (line.contains("SlowOp")) {
                slowOpCount++;
            }
        }
        Assert.assertEquals("Should have 3 slow operations in output", 3, slowOpCount);
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
}
