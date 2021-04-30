package com.walmart.org.dataflow;

import com.walmart.org.dataflow.fn.*;
import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.objects.GameRecord;
import com.walmart.org.dataflow.objects.Result;
import com.walmart.org.dataflow.options.ExecutorOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;
import java.util.List;

public class Executor {
    public static void main(String[] args) throws Exception {
        ExecutorOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ExecutorOptions.class);

        processData(options);
    }

    static State processData(ExecutorOptions options) throws IOException {
        final TupleTag<GameRecord> companyConsoleTupleTag = new TupleTag<GameRecord>(){};

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> consoleRawData = pipeline
                .apply("Reading input console data",
                        TextIO.read().from(options.getInputConsoleData()));

        PCollection<String> resultRawData = pipeline
                .apply("Reading input result data",
                        TextIO.read().from(options.getInputResultData()));

        PCollection<Console> consoleData = consoleRawData
                .apply("Splitting console data",
                        ParDo.of(new SplitConsoleFn()));

        PCollection<Result> resultData = resultRawData
                .apply("Splitting result data",
                        ParDo.of(new SplitResultFn()));

        final PCollectionView<List<Console>> consoleView = consoleData
                .apply("Converting Console as view",
                        View.asList());

        PCollection<GameRecord> gameData = resultData
                .apply("Joining Console and Result data",
                        ParDo.of(new JoinDataFn(consoleView)).withSideInputs(consoleView));

        //group data by key (company_console and console)
        PCollection<KV<String,GameRecord>> companyConsoleKV = gameData
                .apply("Company_Console as KV",
                        ParDo.of(new CompanyConsoleKVfn()));

        PCollection<KV<String,GameRecord>> consoleKV = gameData
                .apply("Console as KV",
                        ParDo.of(new ConsoleKVFn()));

        PCollection<KV<String, CoGbkResult>> companyConsoleKVGrouped = KeyedPCollectionTuple
                .of(companyConsoleTupleTag,companyConsoleKV)
                .apply("Group by company_console", CoGroupByKey.create());

        PCollection<GameRecord> companyConsoleWorstGames = companyConsoleKVGrouped
                .apply("Getting worst games by company_console",
                        ParDo.of(new TopWorstGamesFn(companyConsoleTupleTag, options.getNTop(),false)));

        PCollection<GameRecord> companyConsoleBestGames = companyConsoleKVGrouped
                .apply("Getting top games by company_console",
                        ParDo.of(new TopWorstGamesFn(companyConsoleTupleTag, options.getNTop(),true)));

        gameData.apply("write",
                AvroIO.write(GameRecord.class).to(options.getOutputData()).withNumShards(1));





        return pipeline.run().waitUntilFinish();

    }
}
