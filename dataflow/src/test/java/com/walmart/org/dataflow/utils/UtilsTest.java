package com.walmart.org.dataflow.utils;

import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.objects.Result;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class UtilsTest {

    @Test
    public void getCompanyNameTest(){
        Console console1 = new Console();
        Console console2 = new Console();
        Result result = new Result();

        console1.setConsole("switch");
        console1.setCompany("Nintendo");

        console2.setConsole("PS3");
        console2.setCompany("Sony");

        result.setDate("\"Sep 17, 2013\"");
        result.setName("Grand Theft Auto V");
        result.setConsole("PS3");
        result.setUserscore((float) 8.3);
        result.setMetascore(97);

        ArrayList<Console> consoleList = new ArrayList<Console>();
        consoleList.add(console1);
        consoleList.add(console2);

        Assert.assertEquals("Sony", Utils.getCompanyName(consoleList,result.getConsole()));

    }

    @Test
    public void splitResultRecord(){
        String[] expectedArray = new String[]{"63","Sir, You Are, Being Hunted","PC","6.5","May 2, 2014"};
        String record = "63,\"Sir, You Are, Being Hunted\",PC,6.5,\"May 2, 2014\"";

        Assert.assertArrayEquals(expectedArray,Utils.splitResultRecord(record));
    }


}
