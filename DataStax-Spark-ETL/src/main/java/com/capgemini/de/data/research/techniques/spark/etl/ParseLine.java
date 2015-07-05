package com.capgemini.de.data.research.techniques.spark.etl;

import com.csvreader.CsvReader;
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParseLine {


    // constant

    @NotNull
    private final static String PATH = "/Users/daniel/Dev/Code/local/DataStax-Spark-ETL/DataStax-Spark-ETL" +
            "/src/main/resources/flights_from_pg_top100.csv";


    // static methods

    @NotNull
    public static List<String[]> parseLines(@NotNull final List<String[]> lines) {

        @NotNull final List<String[]> res = new ArrayList<String[]>(lines.size());

        for (final String[] line : lines) {
            res.add(parseSingleLine(line));
        }

        return res;
    }

    @NotNull
    private static String[] parseSingleLine(@NotNull final String[] line) {

        return line;
    }

    @NotNull
    public static List<String[]> getRawText() {

        CsvReader csvReader = null;

        try {
            csvReader = new CsvReader(new FileReader(PATH));
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }

        assert null != csvReader;

        return getRawText(csvReader);
    }

    @NotNull
    public static List<String[]> getRawText(@NotNull final CsvReader csvReader) {

        final List<String[]> res = new ArrayList<String[]>(4096 * 1024);

        try {
            while (csvReader.readRecord()) {

                res.add(csvReader.getValues());
            }

        } catch (final IOException e) {
            e.printStackTrace();
        }

        return res;
    }

    @NotNull
    public static String toText(@NotNull List<String[]> strings) {

        final StringBuilder res = new StringBuilder();

        for (final String[] l : strings) {

            for (final String c : l) {
                res.append(c).append("\t");
            }

            res.append("\n");
        }

        return res.toString();
    }
}
