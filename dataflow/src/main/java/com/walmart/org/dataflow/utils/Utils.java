package com.walmart.org.dataflow.utils;

import com.walmart.org.dataflow.objects.Console;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Utils {

    /**
     * Method used to get Company name from Console Data.
     * @param consoleList list of console records.
     * @param consoleName console name.
     * @return company name.
     */
    public static String getCompanyName(List<Console> consoleList, String consoleName){
        for(Console console : consoleList){
            if(consoleName.equalsIgnoreCase(console.getConsole())){
                return console.getCompany();
            }
        }
        return null;
    }

    /**
     * Method created to split Result data, the main purpose for this code is
     * when the field "name" contains (or not) double quotes.
     * To split the record, in first place the string is splitted by double quote,
     * then only the odd locations are splitted by comma.
     * @param record Result record.
     * @return all fields in a String[].
     */
    public static String[] splitResultRecord(String record){
        ArrayList<String> fields = new ArrayList<>();
        String[] firstSplit = record.split("\"");

        for (int i = 0; i < firstSplit.length; i++) {
            if(i%2 == 0){
                if(firstSplit[i].startsWith(",")){ firstSplit[i] = firstSplit[i].substring(1); }
                if(firstSplit[i].endsWith(",")){ firstSplit[i] = firstSplit[i].substring(0,firstSplit[i].length()-1); }
                String[] b = firstSplit[i].split(",");
                fields.addAll(Arrays.asList(b));
            } else {
                fields.add(firstSplit[i]);
            }
        }
        return fields.toArray(new String[0]);
    }

}
