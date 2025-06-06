---
title: "Testing"
weight: 100
type: docs
aliases:
  - /dev/stream/testing.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Testing

Testing is an integral part of every software development process as such Apache Flink comes with tooling to test your application code on multiple levels of the testing pyramid.

## Testing User-Defined Functions

Usually, one can assume that Flink produces correct results outside of a user-defined function. Therefore, it is recommended to test those classes that contain the main business logic with unit tests as much as possible.

### Unit Testing Stateless, Timeless UDFs


For example, let's take the following stateless `MapFunction`.

```java
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}
```

It is very easy to unit test such a function with your favorite testing framework by passing suitable arguments and verifying the output.
        
```java
public class IncrementMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();

        // call the methods that you have implemented
        assertEquals(3L, incrementer.map(2L));
    }
}
```

Similarly, a user-defined function which uses an `org.apache.flink.util.Collector` (e.g. a `FlatMapFunction` or `ProcessFunction`) can be easily tested by providing a mock object instead of a real collector.  A `FlatMapFunction` with the same functionality as the `IncrementMapFunction` could be unit tested as follows.

```java
public class IncrementFlatMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();

        Collector<Integer> collector = mock(Collector.class);

        // call the methods that you have implemented
        incrementer.flatMap(2L, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(3L);
    }
}
```

### Unit Testing Stateful or Timely UDFs & Custom Operators

Testing the functionality of a user-defined function, which makes use of managed state or timers is more difficult because it involves testing the interaction between the user code and Flink's runtime.
For this Flink comes with a collection of so called test harnesses, which can be used to test such user-defined functions as well as custom operators:

* `OneInputStreamOperatorTestHarness` (for operators on `DataStream`s)
* `KeyedOneInputStreamOperatorTestHarness` (for operators on `KeyedStream`s)
* `TwoInputStreamOperatorTestHarness` (for operators of `ConnectedStreams` of two `DataStream`s)
* `KeyedTwoInputStreamOperatorTestHarness` (for operators on `ConnectedStreams` of two `KeyedStream`s)

To use the test harnesses a set of additional dependencies is needed. Refer to the 
[configuration]({{< ref "docs/dev/configuration/testing" >}}) section for more detail. 

Now, the test harnesses can be used to push records and watermarks into your user-defined functions or custom operators, control processing time and finally assert on the output of the operator (including side outputs).

```java

public class StatefulFlatMapTest {
    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private StatefulFlatMap statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMapFunction();

        // wrap user defined function into a the corresponding operator
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulFlatMapFunction() throws Exception {

        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement(2L, 100L);

        //trigger event time timers by advancing the event time of the operator with a watermark
        testHarness.processWatermark(100L);

        //trigger processing time timers by advancing the processing time of the operator directly
        testHarness.setProcessingTime(100L);

        //retrieve list of emitted records for assertions
        assertThat(testHarness.getOutput(), containsInExactlyThisOrder(3L));

        //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
        //assertThat(testHarness.getSideOutput(new OutputTag<>("invalidRecords")), hasSize(0))
    }
}

```

`KeyedOneInputStreamOperatorTestHarness` and `KeyedTwoInputStreamOperatorTestHarness` are instantiated by additionally providing a `KeySelector` including `TypeInformation` for the class of the key.

```java

public class StatefulFlatMapFunctionTest {
    private OneInputStreamOperatorTestHarness<String, Long, Long> testHarness;
    private StatefulFlatMap statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMapFunction();

        // wrap user defined function into a the corresponding operator
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction), new MyStringKeySelector(), Types.STRING);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    //tests

}

```

Many more examples for the usage of these test harnesses can be found in the Flink code base, e.g.:

* `org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest` is a good example for testing operators and user-defined functions, which depend on processing or event time.

<span class="label label-info">Note</span> Be aware that `AbstractStreamOperatorTestHarness` and its derived classes are currently not part of the public API and can be subject to change.

#### Unit Testing ProcessFunction

Given its importance, in addition to the previous test harnesses that can be used directly to test a `ProcessFunction`, Flink provides a test harness factory named `ProcessFunctionTestHarnesses` that allows for easier test harness instantiation. Considering this example:

<span class="label label-info">Note</span> Be aware that to use this test harness, you also need to introduce the dependencies mentioned in the last section.

```java
public static class PassThroughProcessFunction extends ProcessFunction<Integer, Integer> {

	@Override
	public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
        out.collect(value);
	}
}
```

It is very easy to unit test such a function with `ProcessFunctionTestHarnesses` by passing suitable arguments and verifying the output.

```java
public class PassThroughProcessFunctionTest {

    @Test
    public void testPassThrough() throws Exception {

        //instantiate user-defined function
        PassThroughProcessFunction processFunction = new PassThroughProcessFunction();

        // wrap user defined function into a the corresponding operator
        OneInputStreamOperatorTestHarness<Integer, Integer> harness = ProcessFunctionTestHarnesses
        	.forProcessFunction(processFunction);

        //push (timestamped) elements into the operator (and hence user defined function)
        harness.processElement(1, 10);

        //retrieve list of emitted records for assertions
        assertEquals(harness.extractOutputValues(), Collections.singletonList(1));
    }
}
```

For more examples on how to use the `ProcessFunctionTestHarnesses` in order to test the different flavours of the `ProcessFunction`, e.g. `KeyedProcessFunction`, `KeyedCoProcessFunction`, `BroadcastProcessFunction`, etc, the user is encouraged to look at the `ProcessFunctionTestHarnessesTest`.

## Testing Flink Jobs

### JUnit Rule `MiniClusterWithClientResource`

Apache Flink provides a JUnit rule called `MiniClusterWithClientResource` for testing complete jobs against a local, embedded mini cluster.
called `MiniClusterWithClientResource`.

To use `MiniClusterWithClientResource` one additional dependency (test scoped) is needed.

{{< artifact flink-test-utils withTestScope >}}

Let us take the same simple `MapFunction` as in the previous sections.

```java
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}
```

A simple pipeline using this `MapFunction` can now be tested in a local Flink cluster as follows.

```java
public class ExampleIntegrationTest {

     @ClassRule
     public static MiniClusterWithClientResource flinkCluster =
         new MiniClusterWithClientResource(
             new MiniClusterResourceConfiguration.Builder()
                 .setNumberSlotsPerTaskManager(2)
                 .setNumberTaskManagers(1)
                 .build());

    @Test
    public void testIncrementPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements(1L, 21L, 22L)
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSink.values.containsAll(2L, 22L, 23L));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Long value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
```

A few remarks on integration testing with `MiniClusterWithClientResource`:

* In order not to copy your whole pipeline code from production to test, make sources and sinks pluggable in your production code and inject special test sources and test sinks in your tests.

* The static variable in `CollectSink` is used here because Flink serializes all operators before distributing them across a cluster.
Communicating with operators instantiated by a local Flink mini cluster via static variables is one way around this issue.
Alternatively, you could write the data to files in a temporary directory with your test sink.

* You can implement a custom *parallel* source function for emitting watermarks if your job uses event time timers.

* It is recommended to always test your pipelines locally with a parallelism > 1 to identify bugs which only surface for the pipelines executed in parallel.

* Prefer `@ClassRule` over `@Rule` so that multiple tests can share the same Flink cluster. Doing so saves a significant amount of time since the startup and shutdown of Flink clusters usually dominate the execution time of the actual tests.

* If your pipeline contains custom state handling, you can test its correctness by enabling checkpointing and restarting the job within the mini cluster. For this, you need to trigger a failure by throwing an exception from (a test-only) user-defined function in your pipeline.

{{< top >}}
