package org.digga.bidb.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Sergey Buglivykh at 08.12.11 1:14
 */
public class TextUtils {

    public StringBuilder buildTable(List<String> columns, List<List<String>> rows, int maxColumnWidth) {
        int[] maxWidths = new int[columns.size()];
        Arrays.fill(maxWidths, 0);

        // find max width of each column
        for (int c = 0; c < maxWidths.length; c++) {
            if (columns.get(c).length() > maxWidths[c]) {
                maxWidths[c] = columns.get(c).length();
            }
        }

        if (rows != null) {
            for (int c = 0; c < maxWidths.length; c++) {
                for (List<String> row : rows) {
                    String tc = row.get(c);
                    int len = tc == null ? 0 : tc.length();
                    if (len > maxWidths[c]) {
                        maxWidths[c] = len;
                        maxWidths[c] = maxWidths[c] > maxColumnWidth ? maxColumnWidth : maxWidths[c];
                    }
                }
            }
        }

        // print columns
        StringBuilder buff = new StringBuilder();
        printDelimeter(maxWidths, buff);
        buff.append("|");
        for (int c = 0; c < maxWidths.length; c++) {
            String sz = columns.get(c);
            sz = formatString(sz, " ", maxWidths[c] + 1);
            buff.append(" ").append(sz).append("|");
        }
        buff.append("\n");
        printDelimeter(maxWidths, buff);

        // print rows
        if (rows != null) {
            for (List<String> row : rows) {

                buff.append("|");
                for (int c = 0; c < maxWidths.length; c++) {
                    String sz = row.get(c);
                    sz = formatString(sz, " ", maxWidths[c] + 1);
                    buff.append(" ").append(sz).append("|");
                }
                buff.append("\n");
                printDelimeter(maxWidths, buff);
            }
        }

        buff.append("Total rows: " + (rows == null ? "0" : rows.size()) +"\n");

        return buff;
    }

    private void printDelimeter(int[] maxWidths, StringBuilder buff) {
        buff.append("+");
        for (int i = 0; i < maxWidths.length; i++) {
            String sz = formatString("", "-", maxWidths[i] + 2);
            buff.append(sz).append("+");
        }
        buff.append("\n");
    }

    public String formatString(String sz, String symb, int maxLen) {
        sz = sz == null ? "" : sz;
        if (sz.length() > maxLen) {
            sz = sz.substring(0, maxLen > 3 ? (maxLen - 3) : maxLen);
            sz = maxLen > 3 ? (sz+"...") : sz;
            return sz;
        }

        while (sz.length() < maxLen) {
            sz = sz + symb;
        }
        return sz;
    }

    private List<Long> crc(String sz) {
        int len = 4;

        if (sz.length() < len) {
            return null;
        }

        List<Long> result = new ArrayList<Long>();
        for (int s = 0; s < sz.length() - (len-1); s++) {
            long sum = 0;
            for (int i = 0; i < len; i++) {
                int c = sz.charAt(s+i);
                sum += Math.pow((double)c/100d, (len-i)*3);
                //sum += c * (len-i);
            }
            result.add(sum);
        }

        if (result.size() > 1) {
            result.remove(result.size() - 1);
        }

        return result;
    }

}

