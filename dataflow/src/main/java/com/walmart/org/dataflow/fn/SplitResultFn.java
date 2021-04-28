package com.walmart.org.dataflow.fn;


import com.walmart.org.dataflow.objects.Result;
import org.apache.beam.sdk.transforms.DoFn;

public class SplitResultFn extends DoFn<String, Result> {

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<Result> output){
        String[] resultRaw = new String[5];
        Result result = new Result();

        // Condition to skip csv header
        if(!input.equals("metascore,name,console,userscore,date")){
            resultRaw = input.split(",");

            result.setName(resultRaw[1]);
            result.setConsole(resultRaw[2]);
            result.setDate(resultRaw[4]);

            try{
                result.setMetascore(Integer.parseInt(resultRaw[0]));
                result.setUserscore(Float.parseFloat(resultRaw[3]));
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Please check your data, Metascore or Userscore does not have the correct format.");
            }

            output.output(result);
        }
    }

}
