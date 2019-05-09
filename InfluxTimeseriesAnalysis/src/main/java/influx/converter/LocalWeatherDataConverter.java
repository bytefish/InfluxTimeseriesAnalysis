// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package influx.converter;

import model.LocalWeatherData;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LocalWeatherDataConverter {

    public static BatchPoints convert(List<LocalWeatherData> localWeatherDataList, String database, String retentionPolicy) {

        List<Point> points = localWeatherDataList.stream()
                .map(x -> convert(x))
                .collect(Collectors.toList());

        return BatchPoints
                .database(database)
                .tag("async", "true")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .points(points)
                .build();
    }

    public static Point convert(model.LocalWeatherData source) {

        String wban = source.getStation().getWban();
        LocalDateTime dateTime = source.getDate().atTime(source.getTime());
        Float temperature = source.getTemperature();
        Float windSpeed = source.getWindSpeed();
        Float stationPressure = source.getStationPressure();
        String skyCondition = source.getSkyCondition();


        return Point.measurement("weather_measurement")
                .time(dateTime.toInstant(ZoneOffset.ofHours(0)).toEpochMilli(), TimeUnit.MILLISECONDS)
                .addField("temperature", temperature)
                .addField("wind_speed", windSpeed)
                .addField("station_pressure", stationPressure)
                .addField("sky_condition", skyCondition)
                .tag("wban", wban)
                .build();
    }
}
