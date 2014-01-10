package com.mary.mapr;

import sun.management.HotspotMemoryMBean;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

// Informationstring aggregates from string[4], where string[0] = start
//                                                    string[1] = end
//                                                    string[2] = datetime
//                                                    string[3] = length
public class InformationString {

    private String start = "";
    private String end = "";
    private Date datetime = new Date();
    private String length = "";
    private Integer state = -1;

    public InformationString(String[] string) {

        start = string[0];
        end = string[1];
        length = string[3];
        // Date format example: "14.08.2013 00:07:32"
        try {
            datetime = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss", Locale.ENGLISH).parse(string[2]);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        state = callCompare(start, end);
    }


    private Integer callCompare(String start, String end) {
        String startCode = "";
        String endCode = "";
        

        return null;
    }

    public String getStart(){
        return start;
    }
    public String getEnd() {
        return end;
    }
    public Date getDatetime() {
        return datetime;
    }
    public String getLength() {
        return length;
    }
    public Integer getHours() {
        return datetime.getHours();
    }
}