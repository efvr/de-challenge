package com.walmart.org.dataflow.fn;

import com.walmart.org.dataflow.objects.Console;
import org.apache.beam.sdk.transforms.DoFn;

public class SplitConsoleFn extends DoFn<String, Console> {

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<Console> output){
        String[] consoleRaw = new String[2];
        Console console = new Console();

        // Condition to skip csv header
        if(!input.equals("console,company")){
            consoleRaw = input.split(",");

            console.setConsole(consoleRaw[0]);
            console.setCompany(consoleRaw[1]);

            output.output(console);
        }
    }
}
