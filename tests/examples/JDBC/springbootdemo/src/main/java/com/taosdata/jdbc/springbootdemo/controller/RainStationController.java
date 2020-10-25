package com.taosdata.jdbc.springbootdemo.controller;

import com.taosdata.jdbc.springbootdemo.domain.Rainfall;
import com.taosdata.jdbc.springbootdemo.service.RainStationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/rainstation")
public class RainStationController {
    @Autowired
    private RainStationService service;

    private int keep = 7665;
    private int days = 120;
    private int blocks = 30;

    @GetMapping("/init")
    public boolean init(@RequestParam("stationCount") int stationCount) {
        // create database rainstation keep 7665 days 120 blocks 30
        service.createDb(keep, days, blocks);
        // create table monitoring(ts timestamp, rainfall float) tags(station_code BINARY(8), station_name NCHAR(10))
        service.createSuperTable();
        // create table s_xxx using monitoring tags("s_xxxx","北京");
        service.createSubTables(stationCount);
        return true;
    }

    @GetMapping("/generate")
    public long generate(@RequestParam("stationCount") int stationCount, @RequestParam("timeGap") int timeGap, @RequestParam("batchSize") int batchSize) {
        return service.generate(keep, stationCount, timeGap, batchSize);
    }

    @GetMapping("/latestHour")
    public List<Rainfall> latestHour() {
        return service.latestHour();
    }

    @GetMapping("/latestDay")
    public List<Rainfall> latestDay() {
        return service.latestDay();
    }

    @GetMapping("/latestWeek")
    public List<Rainfall> latestWeek() {
        return service.latestWeek();
    }

    @GetMapping("/latestMonth")
    public List<Rainfall> latestMonth() {
        return service.latestMonth();
    }

    @GetMapping("/latestYear")
    public List<Rainfall> latestYear() {
        return service.latestYear();
    }

    @DeleteMapping

    public boolean dropDatabase() {
        return service.dropDatabase();
    }

}
