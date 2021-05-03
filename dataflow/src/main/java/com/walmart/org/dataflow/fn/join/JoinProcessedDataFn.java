package com.walmart.org.dataflow.fn.join;

import com.walmart.org.dataengchallenge.GameRecord;
import com.walmart.org.dataengchallenge.GameWithoutCompanyConsole;
import com.walmart.org.dataengchallenge.OutputRecord;
import com.walmart.org.dataengchallenge.TopWorstByCompanyConsole;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class receive the 4 categories of games
 * The top/worst for all consoles.
 * The top/worst for each console/company
 * The output is a OutputRecord object.
 */
public class JoinProcessedDataFn extends DoFn<KV<String, CoGbkResult>, OutputRecord> {
    TupleTag<KV<String, List<GameRecord>>> topNGamesByCompanyConsoleTupleTag =
            new TupleTag<KV<String, List<GameRecord>>>(){};
    TupleTag<KV<String, List<GameRecord>>> worstNGamesByCompanyConsoleTupleTag =
            new TupleTag<KV<String, List<GameRecord>>>(){};
    TupleTag<List<GameRecord>> topNGamesTupleTag =
            new TupleTag<List<GameRecord>>(){};
    TupleTag<List<GameRecord>> worstNGamesTupleTag =
            new TupleTag<List<GameRecord>>(){};

    public JoinProcessedDataFn(
            TupleTag<KV<String, List<GameRecord>>> topNGamesByCompanyConsoleTupleTag,
            TupleTag<KV<String, List<GameRecord>>> worstNGamesByCompanyConsoleTupleTag,
            TupleTag<List<GameRecord>> topNGamesTupleTag,
            TupleTag<List<GameRecord>> worstNGamesTupleTag){
        this.topNGamesByCompanyConsoleTupleTag = topNGamesByCompanyConsoleTupleTag;
        this.worstNGamesByCompanyConsoleTupleTag = worstNGamesByCompanyConsoleTupleTag;
        this.topNGamesTupleTag = topNGamesTupleTag;
        this.worstNGamesTupleTag = worstNGamesTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        KV<String,CoGbkResult> elements = c.element();
        OutputRecord outputRecord = new OutputRecord();

        Iterable<KV<String, List<GameRecord>>>  topNGamesByCompanyConsoleIterable =
                elements.getValue().getAll(this.topNGamesByCompanyConsoleTupleTag);

        Iterable<KV<String, List<GameRecord>>>  worstNGamesByCompanyConsoleIterable =
                elements.getValue().getAll(this.worstNGamesByCompanyConsoleTupleTag);

        Iterable<List<GameRecord>> topNGamesIterable =
                elements.getValue().getAll(this.topNGamesTupleTag);

        Iterable<List<GameRecord>> worstNGamesIterable =
                elements.getValue().getAll(this.worstNGamesTupleTag);

        List<GameRecord> topNGamesList = topNGamesIterable.iterator().next();
        List<GameRecord> worstNGamesList = worstNGamesIterable.iterator().next();
        List<TopWorstByCompanyConsole> topByCompanyConsoleList=
                getTopWorstGamesByCompanyConsole(topNGamesByCompanyConsoleIterable);
        List<TopWorstByCompanyConsole> worstByCompanyConsoleList=
                getTopWorstGamesByCompanyConsole(worstNGamesByCompanyConsoleIterable);

        outputRecord.setTopGames(topNGamesList);
        outputRecord.setWorstGames(worstNGamesList);
        outputRecord.setTopGamesByCompanyConsole(topByCompanyConsoleList);
        outputRecord.setWorstGamesByCompanyConsole(worstByCompanyConsoleList);

        c.output(outputRecord);
    }

    /**
     * This method is used to get
     * Top/Worst games for each console/company.
     *
     * @param topNGamesByCompanyConsoleIterable
     * @return a List of TopWorstByCompanyConsole object.
     */
    private List<TopWorstByCompanyConsole> getTopWorstGamesByCompanyConsole(
            Iterable<KV<String, List<GameRecord>>> topNGamesByCompanyConsoleIterable) {
        List<TopWorstByCompanyConsole> topByCompanyConsoleList = new ArrayList<TopWorstByCompanyConsole>();

        for (KV<String, List<GameRecord>> topGamesByCompanyConsoleKV : topNGamesByCompanyConsoleIterable) {
            TopWorstByCompanyConsole topByCompanyConsoleRecord = new TopWorstByCompanyConsole();
            String[] keys = Objects.requireNonNull(topGamesByCompanyConsoleKV.getKey()).split("_");

            topByCompanyConsoleRecord.setCompany(keys[0]);
            topByCompanyConsoleRecord.setConsole(keys[1]);
            topByCompanyConsoleRecord.setGamelist(transformList(Objects.requireNonNull(topGamesByCompanyConsoleKV.getValue())));

            topByCompanyConsoleList.add(topByCompanyConsoleRecord);
        }

        return topByCompanyConsoleList;
    }


    /**
     * This method is used to transform a GameRecord
     * to GameWithoutCompanyConsole, because both fields
     * company and console are removed from GameRecord
     * and showed out of the list.
     * @param gameRecordList
     * @return
     */
    private List<GameWithoutCompanyConsole> transformList(List<GameRecord> gameRecordList){
        List<GameWithoutCompanyConsole> gameList = new ArrayList<GameWithoutCompanyConsole>();

        for(GameRecord gameRecord : gameRecordList){
            GameWithoutCompanyConsole game = new GameWithoutCompanyConsole();
            game.setName(gameRecord.getName());
            game.setDate(gameRecord.getDate());
            game.setMetaScore(gameRecord.getMetaScore());
            game.setUserScore(gameRecord.getUserScore());

            gameList.add(game);
        }
        return gameList;
    }

}
