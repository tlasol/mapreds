package com.mary.mapr;

import com.mary.mapr.reports.Durations;
import com.mary.mapr.reports.Hours;
import com.mary.mapr.reports.InterCodeStats;
import com.mary.mapr.reports.InterStats;

public class MapReduce {

    public static void main(String[] args) throws Exception {

        String[] arguments = new String[3];
        arguments[0] = "job";
        arguments[1] = args[1];

        if (args[0].equals("durations")){
            Durations.main(args);
        }
        if (args[0].equals("hours")){
            Hours.main(args);
        }
        if (args[0].equals("intercode")){
            InterCodeStats.main(args);
        }
        if (args[0].equals("interstats")){
            InterStats.main(args);
        }
        if (args[0].equals("all")){
            arguments[2] = "durations";
            Durations.main(arguments);

            arguments[2] = "hours";
            Hours.main(arguments);

            arguments[2] = "intercode";
            InterCodeStats.main(arguments);

            arguments[2] = "interstats";
            InterStats.main(arguments);
        }

    }
}