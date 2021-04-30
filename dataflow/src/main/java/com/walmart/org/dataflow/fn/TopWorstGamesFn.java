package com.walmart.org.dataflow.fn;

import com.walmart.org.dataflow.objects.GameRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class is used to return the top/worst games
 * this will depend of the parameters in the constructor.
 * This class can return more (or less) than 10 games,
 * you can modify the default value in ExecutorOptions
 * in "nTop" parameter.
 */
public class TopWorstGamesFn extends DoFn<KV<String, CoGbkResult>, GameRecord> {

    private TupleTag<GameRecord> gameRecordTupleTag = new TupleTag<GameRecord>(){};
    private final Integer nTop;
    private final Boolean topGames;

    public TopWorstGamesFn(TupleTag<GameRecord> gameRecordTupleTag, Integer nTop, Boolean topGames){
        this.gameRecordTupleTag = gameRecordTupleTag;
        this.nTop = nTop;
        this.topGames = topGames;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        KV<String, CoGbkResult> elements = c.element();
        Map<Float, GameRecord> ascMap = new TreeMap<Float, GameRecord>();


        Iterable<GameRecord> gameRecordIterable = elements
                .getValue()
                .getAll(this.gameRecordTupleTag);

        for(GameRecord element : gameRecordIterable){
            //skipping "tbd" score
            if(element.getUserscore() != Float.MIN_VALUE) {
                ascMap.put(element.getUserscore(),element);
            }
        }

        if(topGames){
            Map<Float, GameRecord> dscMap = bestGamesOrder(ascMap);
            returnTopWorstGames(c, dscMap, nTop);
        } else {
            returnTopWorstGames(c, ascMap, nTop);
        }
    }

    /**
     * This Method return the top/worst 10 games
     * (you can change the number in ExecutorOptions nTop)
     * as PCollection of GameRecord.
     * @param map asc or desc MAP.
     * @param nTop number of top/worst games that you want show.
     */
    private void returnTopWorstGames(ProcessContext c, Map<Float, GameRecord> map, Integer nTop) {
        int count=0;
        for (Map.Entry<Float, GameRecord> mapData : map.entrySet()) {
            if (count < nTop) {
                //c.output(mapData.getValue());
                System.out.println("-[" + count + "] " + "Key : " + mapData.getKey() + " Value : " + mapData.getValue().getCompany() + "_" +
                        mapData.getValue().getConsole() + " " + mapData.getValue().getName() + " ");
            } else {
                break;
            }
            count++;
        }
    }

    /**
     * This method takes a MAP order  in asc order
     * and return a MAP in desc order.
     *
     * @param ascMap treeMap order in asc way.
     * @return
     */
    public Map<Float, GameRecord> bestGamesOrder(Map<Float, GameRecord> ascMap){
        Map<Float, GameRecord> dscMap= new TreeMap<Float, GameRecord>(new Comparator<Float>() {
            @Override
            public int compare(Float o1, Float o2) {
                return o2.compareTo(o1);
            }
        });
        dscMap.putAll(ascMap);
        return dscMap;
    }

}
