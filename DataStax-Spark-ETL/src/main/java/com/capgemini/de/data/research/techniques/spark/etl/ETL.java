package com.capgemini.de.data.research.techniques.spark.etl;

public final class ETL {

    public static void main(final String[] args) {

        final String strings = ParseLine.toText(ParseLine.parseLines(ParseLine.getRawText()));
        System.out.println(strings);
    }
}
