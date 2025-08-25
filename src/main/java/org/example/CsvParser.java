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
}
