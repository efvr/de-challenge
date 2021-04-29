package com.walmart.org.dataflow;

import com.walmart.org.dataflow.fn.JoinDataFn;
import com.walmart.org.dataflow.fn.SplitConsoleFn;
import com.walmart.org.dataflow.fn.SplitResultFn;
import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.objects.GameRecord;
import com.walmart.org.dataflow.objects.Result;
import com.walmart.org.dataflow.options.ExecutorOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import javax.xml.soap.Text;
import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.List;

public class Executor {
    public static void main(String[] args) throws Exception {
        ExecutorOptions options = PipelineOptionsFactory.
                fromArgs(args).
                withValidation().
                as(ExecutorOptions.class);

        processData(options);
    }

    static State processData(ExecutorOptions options) throws IOException {
        TupleTag<Console> consoleTupleTag = new TupleTag<Console>(){};
        TupleTag<Result> resultTupleTag = new TupleTag<Result>(){};

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> consoleRawData = pipeline.apply("Reading input console data",
                TextIO.read().from(options.getInputConsoleData()));

        PCollection<String> resultRawData = pipeline.apply("Reading input result data",
                TextIO.read().from(options.getInputResultData()));

        PCollection<Console> consoleData = consoleRawData.apply("Splitting console data",
                ParDo.of(new SplitConsoleFn()));

        PCollection<Result> resultData = resultRawData.apply("Splitting result data",
                ParDo.of(new SplitResultFn()));

        final PCollectionView<List<Console>> view = consoleData.apply(View.asList());

        PCollection<GameRecord> resultingPCollection =
                resultData.apply(ParDo
                        .of(new JoinDataFn(view)).withSideInputs(view));



        resultingPCollection.apply("write",
                AvroIO.write(GameRecord.class).to(options.getOutputData()).withNumShards(1));


        return pipeline.run().waitUntilFinish();

    }
}
