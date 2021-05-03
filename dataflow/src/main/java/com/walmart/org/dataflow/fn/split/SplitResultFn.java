package com.walmart.org.dataflow.fn.split;

import com.walmart.org.dataflow.objects.Result;
import com.walmart.org.dataflow.utils.Utils;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This class receive the raw data of "Result"
 * and split it by "," (without double quotes).
 */
public class SplitResultFn extends DoFn<String, Result> {
    Boolean includeTBDScore;

    /**
     *
     * @param includeTBDScore if you want include or not
     *                        the games with "tbd" userScore.
     *                        The default value is False.
     */
    public SplitResultFn(Boolean includeTBDScore){
        this.includeTBDScore = includeTBDScore;
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<Result> output){
        String[] resultRaw = new String[5];
        Result result = new Result();

        // Condition to skip csv header
        if(!input.equals("metascore,name,console,userscore,date")){
            resultRaw = Utils.splitResultRecord(input);

            if(includeTBDScore){
                try{
                    result.setName(resultRaw[1]);
                    result.setConsole(resultRaw[2]);
                    result.setDate(resultRaw[4]);
                    //check if the value is "tdb", if that is the case, then set Float.MIN_VALUE to userScore
                    //in this way will be easy check all tdb as float type.
                    if(resultRaw[3].equalsIgnoreCase("tbd")) {
                        result.setUserscore(Float.MIN_VALUE);
                    }else {
                        result.setUserscore(Float.parseFloat(resultRaw[3]));
                    }
                    result.setMetascore(Integer.parseInt(resultRaw[0]));
                    output.output(result);
                } catch (NumberFormatException e) {
                    throw new NumberFormatException("Please check your data." +
                            "\nRecord: [" + input + "].");
                }
            } else {
                if(!resultRaw[3].equalsIgnoreCase("tbd")) {
                    try{
                        result.setName(resultRaw[1]);
                        result.setConsole(resultRaw[2]);
                        result.setDate(resultRaw[4]);
                        result.setUserscore(Float.parseFloat(resultRaw[3]));
                        result.setMetascore(Integer.parseInt(resultRaw[0]));
                        output.output(result);
                    } catch (NumberFormatException e) {
                        throw new NumberFormatException("Please check your data." +
                                "\nRecord: [" + input + "].");
                    }
                }
            }
        }
    }



}
