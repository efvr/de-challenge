package com.walmart.org.dataflow.objects;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.annotations.SerializedName;

@DefaultCoder(AvroCoder.class)

public class Console {
    @SerializedName("console")
    String console;

    @SerializedName("company")
    String company;

    public String getConsole() {
        return console;
    }

    public void setConsole(String console) {
        this.console = console;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }


}
