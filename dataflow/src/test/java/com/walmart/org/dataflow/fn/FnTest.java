package com.walmart.org.dataflow.fn;

import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.objects.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FnTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void splitConsoleFnTest() {
        List<String> consoleData = Arrays.asList(
                "console,company",
                "Switch,Nintendo",
                "PS5,Sony");

        PCollection<Console> output =
                pipeline.apply(Create.of(consoleData).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new SplitConsoleFn()));

        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(2L);

        pipeline.run();
    }

    @Test
    public void splitResultFnTest(){

        List<String> resultData = Arrays.asList(
                "97,Grand Theft Auto V,PS3,8.3,\"Sep 17, 2013\"",
                "88,Pokemon Y,3DS,8.6,\"Oct 12, 2013\"");

        PCollection<Result> output =
                pipeline.apply(Create.of(resultData).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new SplitResultFn()));

        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(2L);

        pipeline.run();
    }

    @Test(expected = Pipeline.PipelineExecutionException.class)
    public void splitResultFnThrowExceptionTest(){

        List<String> resultData = Arrays.asList(
                "wrongValue,Grand Theft Auto V,PS3,8.3,\"Sep 17, 2013\"");

        PCollection<Result> output =
                pipeline.apply(Create.of(resultData).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new SplitResultFn()));

        pipeline.run();
    }
}
