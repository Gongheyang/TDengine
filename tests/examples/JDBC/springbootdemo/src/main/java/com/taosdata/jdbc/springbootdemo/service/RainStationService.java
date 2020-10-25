package com.taosdata.jdbc.springbootdemo.service;

import com.taosdata.jdbc.springbootdemo.dao.RainstationMapper;
import com.taosdata.jdbc.springbootdemo.domain.Rainfall;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class RainStationService {

    @Autowired
    private RainstationMapper rainstationMapper;

    private Random random = new Random(System.currentTimeMillis());
    private String[] stationNames = {"北京", "上海", "深圳", "广州", "杭州", "苏州", "成都", "西安", "天津", "厦门", "福州"};

    public boolean createDb(int keep, int days, int blocks) {
        rainstationMapper.dropDatabase("rainstation");

        Map<String, String> map = new HashMap<>();
        map.put("dbname", "rainstation");
        map.put("keep", "" + keep);
        map.put("days", "" + days);
        map.put("blocks", "" + blocks);
        rainstationMapper.creatDatabase(map);
        rainstationMapper.useDatabase("rainstation");
        return true;
    }

    public boolean createSuperTable() {
        try {
            rainstationMapper.createSuperTable("rainstation", "monitoring");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // create table s_xxx using monitoring tags("s_xxxx","北京");
    public void createSubTables(int stationCount) {
        for (int i = 0; i < stationCount; i++) {
            rainstationMapper.createSubTable("s_" + i, "s_" + i, stationNames[random.nextInt(stationNames.length)]);
        }
    }

    public boolean dropDatabase() {
        try {
            rainstationMapper.dropDatabase("rainstation");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public long generate(int keep, int stationCount, long timeGapSec, int batchSize) {
        rainstationMapper.useDatabase("rainstation");
        Instant end = Instant.now();
        Instant start = end.minus(Duration.ofDays(keep - 1));
        final long timeGap = 1000l * timeGapSec;

        if (stationCount < 1000) {
            return insert(start.toEpochMilli(), end.toEpochMilli(), timeGap, stationCount);
        } else {
            return batchInsert(start.toEpochMilli(), end.toEpochMilli(), timeGap, batchSize, stationCount);
        }
    }

    private long insert(long startTime, long endTime, long timeGap, int tableSize) {
        long count = 0;
        for (long ts = startTime; ts < endTime; ts += timeGap) {
            for (int i = 0; i < tableSize; i++) {
                Rainfall rainfall = new Rainfall();
                rainfall.setTs(new Timestamp(ts));
                rainfall.setRainfall(random.nextFloat() * 999);
                rainfall.setStation_code("s_" + i);
                count += rainstationMapper.insertOne(rainfall);
            }
        }
        return count;
    }

    public long batchInsert(long startTime, long endTime, long timeGap, int batchSize, int tableSize) {
        long count = 0;
//        for (long ts = startTime; ts < endTime; ts += (timeGap * batchSize)) {
//            for (int i = 0; i < tableSize; i++) {
//                List<Rainfall> rainfallList = new ArrayList<>(batchSize);
//                for (int j = 0; j < batchSize && (ts + j * timeGap) < endTime; j++) {
//                    Rainfall rainfall = new Rainfall();
//                    rainfall.setTs(new Timestamp(ts + (j * timeGap)));
//                    rainfall.setRainfall(random.nextFloat() * 999);
//                    rainfallList.add(rainfall);
//                }
//                count += rainstationMapper.insertMany("s_" + i, rainfallList);
//            }
//        }

        /***********************************/
        for (int i = 0; i < tableSize; i++) {
            for (long ts = startTime; ts < endTime; ts += (timeGap * batchSize)) {
                List<Rainfall> rainfallList = new ArrayList<>(batchSize);
                for (int j = 0; j < batchSize && (ts + j * timeGap) < endTime; j++) {
                    Rainfall rainfall = new Rainfall();
                    rainfall.setTs(new Timestamp(ts + (j * timeGap)));
                    rainfall.setRainfall(random.nextFloat() * 999);
                    rainfallList.add(rainfall);
                }
                count += rainstationMapper.insertMany("s_" + i, rainfallList);
            }
        }

        return count;
    }

    public List<Rainfall> latestHour() {
        Instant end = Instant.now();
        Instant start = end.minus(Duration.ofHours(1));
        Map<String, Object> map = new HashMap<>();
        map.put("startTime", start.toEpochMilli());
        map.put("endTime", end.toEpochMilli());
        return rainstationMapper.findAll(map);
    }

    public List<Rainfall> latestDay() {
        Instant end = Instant.now();
        Instant start = end.minus(Duration.ofDays(1));
        Map<String, Object> map = new HashMap<>();
        map.put("startTime", start.toEpochMilli());
        map.put("endTime", end.toEpochMilli());
        return rainstationMapper.findAll(map);
    }

    public List<Rainfall> latestWeek() {
        Instant end = Instant.now();
        Instant start = end.minus(Duration.ofDays(7));
        Map<String, Object> map = new HashMap<>();
        map.put("startTime", start.toEpochMilli());
        map.put("endTime", end.toEpochMilli());
        return rainstationMapper.findAll(map);
    }

    public List<Rainfall> latestMonth() {
        Instant end = Instant.now();
        Instant start = end.minus(Duration.ofDays(30));
        Map<String, Object> map = new HashMap<>();
        map.put("startTime", start.toEpochMilli());
        map.put("endTime", end.toEpochMilli());
        return rainstationMapper.findAll(map);
    }

    public List<Rainfall> latestYear() {
        Instant end = Instant.now();
        Instant start = end.minus(Duration.ofDays(365));
        Map<String, Object> map = new HashMap<>();
        map.put("startTime", start.toEpochMilli());
        map.put("endTime", end.toEpochMilli());
        return rainstationMapper.findAll(map);
    }


}