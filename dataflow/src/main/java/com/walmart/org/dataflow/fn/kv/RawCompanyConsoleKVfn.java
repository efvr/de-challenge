package com.walmart.org.dataflow.fn.kv;

import com.walmart.org.dataengchallenge.GameRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * This class take the processed data
 * and returns KV.
 * Creates the new Key, concatenate company
 * and console values.
 *
 * Key: company_console
 * Value: GameRecord
 */
public class RawCompanyConsoleKVfn extends DoFn<GameRecord, KV<String,GameRecord>> {

    @ProcessElement
    public void processElement(@Element GameRecord element, OutputReceiver<KV<String,GameRecord>> output){
        String key = element.getCompany() + "_" + element.getConsole();
        output.output(KV.of(key, element));
    }
}
