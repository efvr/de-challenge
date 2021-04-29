package com.walmart.org.dataflow.fn;


import com.walmart.org.dataflow.objects.Result;
import com.walmart.org.dataflow.utils.Utils;
import org.apache.beam.sdk.transforms.DoFn;

public class SplitResultFn extends DoFn<String, Result> {

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<Result> output){
        String[] resultRaw = new String[5];
        Result result = new Result();

        // Condition to skip csv header
        if(!input.equals("metascore,name,console,userscore,date")){

            //using an specific split because the second field can contain (or not) double quotes.
            resultRaw = Utils.splitResultRecord(input);

            result.setName(resultRaw[1]);
            result.setConsole(resultRaw[2]);
            result.setDate(resultRaw[4]);

            try{
                //check if the value is "tdb", if that is the case, then set Float.MIN_VALUE to userScore
                //in this way will be easy check all tdb as float type.
                if(resultRaw[3].equalsIgnoreCase("tbd")) {
                    result.setUserscore(Float.MIN_VALUE);
                }else {
                    result.setUserscore(Float.parseFloat(resultRaw[3]));
                }
                result.setMetascore(Integer.parseInt(resultRaw[0]));
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Please check your data, Metascore or Userscore does not have the correct format." +
                        "\nRecord: [" + input + "].");
            }


            output.output(result);
        }
    }



}
