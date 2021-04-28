package com.walmart.org.dataflow.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExecutorOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Validation.Required
    String getInputData();
    void setInputData(String inputData);

    @Description("Path of the file to read from")
    String getOutputFormat();
    @Default.String("avro")
    void setOutputFormat(String outputFormat);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutputData();
    void setOutputData(String outputData);

}
