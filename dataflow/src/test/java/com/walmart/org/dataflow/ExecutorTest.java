package com.walmart.org.dataflow;

import com.walmart.org.dataflow.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class ExecutorTest {
    private ExecutorOptions options;

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Before
    public void setUp(){
        PipelineOptionsFactory.register(ExecutorOptions.class);
        options = PipelineOptionsFactory.as(ExecutorOptions.class);
        options = TestPipeline.testingPipelineOptions().as(ExecutorOptions.class);
        options.setInputConsoleData("/home/francisco/Documents/de-challenge/data/consoles.csv");
        options.setInputResultData("/home/francisco/Documents/de-challenge/data/result.csv");
        options.setJobName("test-job");
        options.setOutputData("/home/francisco/Documents/de-challenge/output/");
        options.setNTop(5);
    }

    @Test
    public void executorTest() throws IOException {
       Executor executor = new Executor();
       executor.processData(options);
    }



}
