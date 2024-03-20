package com.bhupixb.flink.source.csv;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * This Flink program reads data from a CSV file with headers and outputs a DataStream of
 * LinkedHashMaps. Each LinkedHashMap represents a row of the CSV file, with keys being the header
 * names and values being the corresponding row values.
 */
public class CsvSourceMain {
    // You can change this to absolute path of your csv file.
    static final String INPUT_FILE =
            Objects.requireNonNull(CsvSourceMain.class.getResource("/input.csv")).getPath();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CsvReaderFormat<LinkedHashMap> csvFormat = getCsvReaderFormat(LinkedHashMap.class);

        FileSource<LinkedHashMap> source =
                FileSource.forRecordStreamFormat(csvFormat, new Path(INPUT_FILE)).build();

        /* Each record of this data stream will be a map where key is the header and value is the actual csv column value.*/
        DataStream<LinkedHashMap> dataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-file-source");

        dataStream.print();

        /*
        Sample output of program:
        3> {first_name=john, age=19, last_name=singh}
        3> {first_name=ronnie, age=30, last_name=col}
        3> {first_name=chris, age=25, last_name=pratt}
        */
        env.execute();
    }

    /**
     * Build a {@link CsvReaderFormat} where you can pass your own CsvSchema. Everything that
     * Jackson supports is supported here: <a
     * href="https://javadoc.io/doc/com.fasterxml.jackson.dataformat/jackson-dataformat-csv/2.12.4/com/fasterxml/jackson/dataformat/csv/CsvSchema.html">
     * Jackson CsvSchema Documentation</a>
     */
    public static <T> CsvReaderFormat<T> getCsvReaderFormat(Class<T> clazz) {
        return CsvReaderFormat.forSchema(
                JacksonMapperFactory::createCsvMapper,
                getCsvSchema(clazz),
                TypeInformation.of(clazz));
    }

    // Build the CsvSchema for the input class
    private static <T> SerializableFunction<CsvMapper, CsvSchema> getCsvSchema(Class<T> clazz) {
        return mapper -> {
            // Deserialize/Map each record/csv-row to class T
            CsvSchema csvSchema = mapper.schemaFor(clazz);
            // Use first row as header from input file.
            csvSchema = csvSchema.withHeader();

            // ... other csv schema configs

            return csvSchema;
        };
    }
}
