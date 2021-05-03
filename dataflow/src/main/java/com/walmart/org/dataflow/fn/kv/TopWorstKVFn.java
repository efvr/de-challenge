package com.walmart.org.dataflow.fn.kv;

import com.walmart.org.dataengchallenge.GameRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.List;

/**
 * This class is used to create a KV
 * to then join the 4 categories of top/worst games.
 * The key must be the same for all categories.
 */
public class TopWorstKVFn extends DoFn<List<GameRecord>, KV<String,List<GameRecord>>> {

    @ProcessElement
    public void processElement(@Element List<GameRecord> element, OutputReceiver<KV<String,List<GameRecord>>> output){
        output.output(KV.of("key", element));
    }

}
