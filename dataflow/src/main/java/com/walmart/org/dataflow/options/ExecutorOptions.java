package com.walmart.org.dataflow.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExecutorOptions extends PipelineOptions {
    @Description("Path of the console files to read from")
    @Validation.Required
    String getInputConsoleData();
    void setInputConsoleData(String inputConsoleData);

    @Description("Path of the result files to read from")
    @Validation.Required
    String getInputResultData();
    void setInputResultData(String inputResultData);

    @Description("Top n° best/worst games. Default value is 10")
    @Default.Integer(10)
    Integer getNTop();
    void setNTop(Integer nTop);

    @Description("If \"tdb\" score should be included. Default is false.")
    @Default.Boolean(false)
    Boolean getIncludeTBDScore();
    void setIncludeTBDScore(Boolean includeTBDScore);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutputData();
    void setOutputData(String outputData);

}
