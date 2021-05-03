package com.walmart.org.dataflow;

import com.walmart.org.dataengchallenge.GameRecord;
import com.walmart.org.dataengchallenge.OutputRecord;
import com.walmart.org.dataflow.fn.comparator.CompareGameByUserScoreTop;
import com.walmart.org.dataflow.fn.comparator.CompareGameByUserScoreWorst;
import com.walmart.org.dataflow.fn.join.JoinProcessedDataFn;
import com.walmart.org.dataflow.fn.join.JoinRawDataFn;
import com.walmart.org.dataflow.fn.kv.RawCompanyConsoleKVfn;
import com.walmart.org.dataflow.fn.kv.TopWorstByCompanyConsoleKVFn;
import com.walmart.org.dataflow.fn.kv.TopWorstKVFn;
import com.walmart.org.dataflow.fn.split.SplitConsoleFn;
import com.walmart.org.dataflow.fn.split.SplitResultFn;
import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.options.ExecutorOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.io.IOException;
import java.util.List;

public class Executor {
    private static final String avroSuffix = ".avro";

    public static void main(String[] args) throws Exception {
        ExecutorOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ExecutorOptions.class);

        processData(options);
    }

    static State processData(ExecutorOptions options) throws IOException {
        final TupleTag<KV<String, List<GameRecord>>> topNGamesByCompanyConsoleTupleTag =
                new TupleTag<KV<String, List<GameRecord>>>(){};
        final TupleTag<KV<String, List<GameRecord>>> worstNGamesByCompanyConsoleTupleTag =
                new TupleTag<KV<String, List<GameRecord>>>(){};
        final TupleTag<List<GameRecord>> topNGamesTupleTag =
                new TupleTag<List<GameRecord>>(){};
        final TupleTag<List<GameRecord>> worstNGamesTupleTag =
                new TupleTag<List<GameRecord>>(){};

        Pipeline pipeline = Pipeline.create(options);

        PCollectionView<List<Console>> consoleView = pipeline
                .apply("Reading input console data",
                        TextIO.read().from(options.getInputConsoleData()))
                .apply("Splitting console data",
                        ParDo.of(new SplitConsoleFn()))
                .apply("console data as view",
                        View.asList());

        PCollection<GameRecord> gameData = pipeline
                .apply("Reading input result data",
                        TextIO.read().from(options.getInputResultData()))
                .apply("Splitting result data",
                        ParDo.of(new SplitResultFn(options.getIncludeTBDScore())))
                .apply("Joining Console and Result data",
                        ParDo.of(new JoinRawDataFn(consoleView)).withSideInputs(consoleView));

        //group data by key (company_console and console)
        PCollection<KV<String,GameRecord>> companyConsoleKV = gameData
                .apply("Company_Console as KV",
                        ParDo.of(new RawCompanyConsoleKVfn()));

        //get top N games by userscore
        PCollection<KV<String, List<GameRecord>>> topNGamesByCompanyConsole = companyConsoleKV
                .apply("Top N Games by company_console",
                        Top.perKey(options.getNTop(), new CompareGameByUserScoreTop()));

        PCollection<KV<String, List<GameRecord>>> worstNGamesByCompanyConsole = companyConsoleKV
                .apply("Worst N Games by company_console",
                        Top.perKey(options.getNTop(), new CompareGameByUserScoreWorst()));

        PCollection<List<GameRecord>> topNGames = gameData
                .apply("Top N Games",
                        Top.of(options.getNTop(), new CompareGameByUserScoreTop()));

        PCollection<List<GameRecord>> worstNGames = gameData
                .apply("Worst N Games",
                        Top.of(options.getNTop(), new CompareGameByUserScoreWorst()));

        //join 4 categories
        PCollection<KV<String,KV<String, List<GameRecord>>>> topNGamesByCompanyConsoleKV = topNGamesByCompanyConsole
                .apply("KV of topNGamesByCompanyConsole",
                        ParDo.of(new TopWorstByCompanyConsoleKVFn()));

        PCollection<KV<String,KV<String, List<GameRecord>>>> worstNGamesByCompanyConsoleKV = worstNGamesByCompanyConsole
                .apply("KV of worstNGamesByCompanyConsole",
                        ParDo.of(new TopWorstByCompanyConsoleKVFn()));

        PCollection<KV<String,List<GameRecord>>> topNGamesKV = topNGames
                .apply("KV of topNGames",
                        ParDo.of(new TopWorstKVFn()));

        PCollection<KV<String,List<GameRecord>>> worstNGamesKV = worstNGames
                .apply("KV of worstNGames",
                        ParDo.of(new TopWorstKVFn()));

        KeyedPCollectionTuple
                .of(topNGamesByCompanyConsoleTupleTag,topNGamesByCompanyConsoleKV)
                .and(worstNGamesByCompanyConsoleTupleTag,worstNGamesByCompanyConsoleKV)
                .and(topNGamesTupleTag,topNGamesKV)
                .and(worstNGamesTupleTag,worstNGamesKV)
                .apply("Joining 4 categories", CoGroupByKey.create())
                .apply("Joning data", ParDo.of(new JoinProcessedDataFn(
                    topNGamesByCompanyConsoleTupleTag,
                    worstNGamesByCompanyConsoleTupleTag,
                    topNGamesTupleTag,
                    worstNGamesTupleTag)))
                .apply("write data", AvroIO
                        .write(OutputRecord.class)
                        .to(options.getOutputData())
                        .withNumShards(1)
                        .withSuffix(avroSuffix));

        return pipeline.run().waitUntilFinish();

    }
}
