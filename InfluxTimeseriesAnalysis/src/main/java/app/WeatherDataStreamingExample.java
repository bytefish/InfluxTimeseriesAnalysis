// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package app;

import csv.model.LocalWeatherData;
import csv.model.Station;
import csv.parser.Parsers;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import influx.converter.LocalWeatherDataConverter;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeatherDataStreamingExample {

    static final String databaseName = "weather_data";
    static final String retentionPolicyName = "weather_data_policy";

    public static void main(String[] args) {


        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");

        // Path to QCLCD CSV Files:
        final Path csvStationDataFilePath = FileSystems.getDefault().getPath("D:\\datasets\\201503station.txt");
        final Path csvLocalWeatherDataFilePath = FileSystems.getDefault().getPath("D:\\datasets\\201503hourly.txt");

        // A map between the WBAN and Station for faster Lookups:
        final Map<String, Station> stationMap = getStationMap(csvStationDataFilePath);

        try (Stream<CsvMappingResult<LocalWeatherData>> csvStream = getLocalWeatherData(csvLocalWeatherDataFilePath)) {

            // Now turn the CSV Stream into the Model Stream:
            Stream<model.LocalWeatherData> localWeatherDataStream = csvStream
                    // Filter only valid entries:
                    .filter(x -> x.isValid())
                    // Now we can work on the Results:
                    .map(x -> x.getResult())
                    // Take only measurements available in the list of stations:
                    .filter(x -> stationMap.containsKey(x.getWban()))
                    // Map into the general Analytics Model:
                    .map(x -> {
                        // Get the matching station now:
                        csv.model.Station station = stationMap.get(x.getWban());
                        // And build the Model:
                        return csv.converter.LocalWeatherDataConverter.convert(x, station);
                    });

            // Turn it into an Observable for simplified Buffering:
            Disposable disposable = Observable.fromIterable(localWeatherDataStream::iterator)
                    // Wait two Seconds or Buffer up to 80000 entities:
                    .buffer(2, TimeUnit.SECONDS, 10000)
                    // Subscribe to the Batches:
                    .subscribe(x ->
                    {
                        // Convert the Batch into InfluxDB BatchPoints:
                        BatchPoints batchPoints = LocalWeatherDataConverter.convert(x, databaseName, retentionPolicyName);

                        // Write the Points:
                        influxDB.write(batchPoints);
                    }, x -> System.err.println(x));

            // Probably not neccessary, but dispose anyway:
            if (!disposable.isDisposed()) {
                disposable.dispose();
            }
        }
    }

    private static Stream<CsvMappingResult<csv.model.LocalWeatherData>> getLocalWeatherData(Path path) {
        return Parsers.LocalWeatherDataParser().readFromFile(path, StandardCharsets.US_ASCII);
    }

    private static Stream<csv.model.Station> getStations(Path path) {
        return Parsers.StationParser().readFromFile(path, StandardCharsets.US_ASCII)
                .filter(x -> x.isValid())
                .map(x -> x.getResult());
    }

    private static Map<String, Station> getStationMap(Path path) {
        try (Stream<csv.model.Station> stationStream = getStations(path)) {
            return stationStream
                    .collect(Collectors.groupingBy(x -> x.getWban()))
                    .entrySet().stream()
                    .map(x -> x.getValue().get(0))
                    .collect(Collectors.toMap(csv.model.Station::getWban, x -> x));
        }
    }
}
