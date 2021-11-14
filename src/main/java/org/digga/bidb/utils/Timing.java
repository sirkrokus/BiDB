package org.digga.bidb.utils;

import java.util.HashMap;
import java.util.Map;

public class Timing {

    private static Map<String, TimingDescriptor> map = new HashMap<>();

    public static void start(String name) {
        TimingDescriptor td = map.get(name);
        if (td == null) {
            td = new TimingDescriptor();
            td.startTime = System.currentTimeMillis();
            map.put(name, td);
        }
        td.callCount++;
        td.t1 = System.currentTimeMillis();
    }

    public static void stop(String name, int period) {
        TimingDescriptor td = map.get(name);
        td.t2 = System.currentTimeMillis();
        td.mean = td.mean + (td.t2 - td.t1);
        td.count++;

        if (td.count != period) {
            return;
        }
        System.out.println("[" + name + "]: count = "+td.callCount+"/"+td.count+", mean = " + (td.mean / td.count) + " ms, total duration = " + (td.t2 - td.startTime) + " ms");
        td.mean = 0;
        td.count = 0;
        td.startTime = System.currentTimeMillis();
    }

    static class TimingDescriptor {
        double mean = 0d;
        double count = 0l;
        int callCount = 0;
        long startTime = 0l;
        long t1 = 0l;
        long t2 = 0l;
    }

}
