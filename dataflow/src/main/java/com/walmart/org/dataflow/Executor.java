package com.walmart.org.dataflow;

import com.walmart.org.dataflow.fn.SplitConsoleFn;
import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.options.ExecutorOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Executor {
    public static void main(String[] args) {
        ExecutorOptions options = PipelineOptionsFactory.
                fromArgs(args).
                withValidation().
                as(ExecutorOptions.class);
    }

    public void processData(ExecutorOptions options){
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> consoleRawData = pipeline.apply("Reading input data",
                TextIO.read().from(options.getInputData()));
        PCollection<Console> consoleData = consoleRawData.apply("Splitting data",
                ParDo.of(new SplitConsoleFn()));



        pipeline.run().waitUntilFinish();

    }
}
