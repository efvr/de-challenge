package com.walmart.org.dataflow;

import com.walmart.org.dataengchallenge.OutputRecord;
import com.walmart.org.dataflow.options.ExecutorOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class ExecutorTest {
    private ExecutorOptions options;
    private static String temporaryOutputPath = null;

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        temporaryOutputPath = temporaryFolder.newFolder("output").getAbsolutePath();

        PipelineOptionsFactory.register(ExecutorOptions.class);
        options = PipelineOptionsFactory.as(ExecutorOptions.class);
        options = TestPipeline.testingPipelineOptions().as(ExecutorOptions.class);
        options.setInputConsoleData("/home/francisco/Documents/de-challenge/data/consoles.csv");
        options.setInputResultData("/home/francisco/Documents/de-challenge/data/result.csv");
        options.setJobName("test-job");
//        options.setOutputData(temporaryOutputPath);
        options.setOutputData("/home/francisco/Documents/de-challenge/output");
        options.setNTop(5);
    }

    @Test
    public void executorTest() throws IOException {
        Executor executor = new Executor();
        PipelineResult.State executionState = executor.processData(options);

//        PCollection<OutputRecord> outputData = pipeline
//                .apply("reading output data",
//                        AvroIO.read(OutputRecord.class).from(options.getOutputData()+"/*"));
//
//        PAssert.that(outputData.apply("Count records", Count.globally())).containsInAnyOrder(1L);

        Assert.assertEquals(PipelineResult.State.DONE,executionState);

        pipeline.run().waitUntilFinish();
    }



}
