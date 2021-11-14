package org.digga.bidb;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log {

    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    private static Pattern pattern = Pattern.compile("(\\{\\})");

    public static void p(String msg, Object... args) {
        print(true, true, msg, args);
    }

    public static void str(String msg, Object... args) {
        print(false, false, msg, args);
    }

    public static void ln() {
        System.out.println();
    }

    private static void print(boolean addCR, boolean addDate, String msg, Object... args) {
        Matcher m = pattern.matcher(msg);
        StringBuffer sb = new StringBuffer();
        int ac = 1;
        while (m.find()) {
            m = m.appendReplacement(sb,"\\%"+ac+"\\$s");
            ac++;
        }
        sb = m.appendTail(sb);
        System.out.printf((addDate ? ("["+sdf.format(new Date())+"] ") : "") + sb.toString() + (addCR ? "\n" : ""), args);
    }

}
