package com.walmart.org.dataflow.fn;

import com.walmart.org.dataflow.objects.GameRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class CompanyConsoleKVfn extends DoFn<GameRecord, KV<String,GameRecord>> {

    @ProcessElement
    public void processElement(@Element GameRecord element, OutputReceiver<KV<String,GameRecord>> output){
        String key = element.getCompany()+"_"+element.getConsole();
        output.output(KV.of(key,element));
    }
}
