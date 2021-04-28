package com.walmart.org.dataflow.objects;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.annotations.SerializedName;

@DefaultCoder(AvroCoder.class)

public class Result {

    @SerializedName("metascore")
    Integer metascore;

    @SerializedName("name")
    String name;

    @SerializedName("console")
    String console;

    @SerializedName("userscore")
    Float userscore;

    @SerializedName("date")
    String date;

    public Integer getMetascore() {
        return metascore;
    }

    public void setMetascore(Integer metascore) {
        this.metascore = metascore;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConsole() {
        return console;
    }

    public void setConsole(String console) {
        this.console = console;
    }

    public Float getUserscore() {
        return userscore;
    }

    public void setUserscore(Float userscore) {
        this.userscore = userscore;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
