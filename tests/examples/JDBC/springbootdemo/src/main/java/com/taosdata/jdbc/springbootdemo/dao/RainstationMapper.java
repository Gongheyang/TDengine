package com.taosdata.jdbc.springbootdemo.dao;

import com.taosdata.jdbc.springbootdemo.domain.Rainfall;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface RainstationMapper {

    int dropDatabase(String dbname);

    int creatDatabase(Map<String, String> map);

    int useDatabase(String dbname);

    boolean createSuperTable(String dbname, String stbName);

    boolean createSubTable(String tableName, String stationCode, String stationName);

    int insertOne(Rainfall rainfall);

    int insertMany(@Param("tableName") String tableName, @Param("rainfalls") List<Rainfall> rainfallList);

    List<Rainfall> findAll(Map<String, Object> condi);
}
