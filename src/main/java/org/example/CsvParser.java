package org.example;

import java.util.ArrayList;
import java.util.List;

public class CsvParser {

    private CsvParser() {}

    /**
     * Parse a line of CSV data into usable strings.
     * Handles quotes and doubled quotes "" within quoted fields.
     */
    public static List<String> parse(String line){
        List<String> result = new ArrayList<>();
        if (line == null) return result;

        int length = line.length();

        if (length > 0 && line.charAt(length - 1) == '\r') {
            line = line.substring(0, length - 1);
            length--;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < length; i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < length && line.charAt(i + 1) == '"') {
                    current.append('"'); // escaped quote
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        return result;
    }

    public static String[] parseSelected(String line, int... indices) {
        if (line == null || indices == null || indices.length == 0) return new String[0];

        int length = line.length();
        if (length > 0 && line.charAt(length - 1) == '\r') {
            line = line.substring(0, length - 1);
            length--;
        }

        int maxIdx = -1;
        for (int idx : indices) {
            if (idx > maxIdx) maxIdx = idx;
        }

        String[] out = new String[indices.length];

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int fieldIdx = 0;

        for (int i = 0; i < length; i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < length && line.charAt(i + 1) == '"') {
                    current.append('"'); // escaped quote
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                // end of field
                captureIfRequested(indices, out, fieldIdx, current);
                fieldIdx++;
                if (fieldIdx > maxIdx && allCaptured(out)) {
                    break;
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        // last field
        if (current.length() > 0 || line.endsWith(",")) {
            captureIfRequested(indices, out, fieldIdx, current);
        }

        return out;
    }

    private static void captureIfRequested(int[] indices, String[] out, int fieldIdx, StringBuilder current) {
        for (int i = 0; i < indices.length; i++) {
            if (indices[i] == fieldIdx) {
                out[i] = current.toString();
                return;
            }
        }
    }

    private static boolean allCaptured(String[] out) {
        for (String s : out) {
            if (s == null) return false;
        }
        return true;
    }
}
