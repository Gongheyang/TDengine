package com.taosdata.jdbc.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class SqlSpeller {

    public static String createSuperTable(String stbname, Map<String, String> fieldsMeta, Map<String, String> tagsMeta) {
        StringBuilder sql = new StringBuilder();
        sql.append("create table " + stbname + " (");
        Iterator<String> keyIterator = fieldsMeta.keySet().iterator();
        for (int i = 0; keyIterator.hasNext(); i++) {
            String key = keyIterator.next();
            if (i == 0)
                sql.append(key + " " + fieldsMeta.get(key));
            else
                sql.append(", " + key + " " + fieldsMeta.get(key));
        }
        sql.append(") tags(");
        keyIterator = tagsMeta.keySet().iterator();
        for (int i = 0; keyIterator.hasNext(); i++) {
            String key = keyIterator.next();
            if (i == 0)
                sql.append(key + " " + tagsMeta.get(key));
            else
                sql.append(", " + key + " " + tagsMeta.get(key));
        }
        sql.append(")");
        return sql.toString();
    }

    public static void main(String[] args) {
        Map<String, String> fieldsMeta = new LinkedHashMap<>();
        fieldsMeta.put("ts", "timestamp");
        fieldsMeta.put("temperature", "float");
        fieldsMeta.put("humidity", "int");
        Map<String, String> tagsMeta = new LinkedHashMap<>();
        tagsMeta.put("location", "nchar(64)");
        tagsMeta.put("tableId", "int");
        String sql = SqlSpeller.createSuperTable("weather", fieldsMeta, tagsMeta);
        System.out.println(sql);
    }

}
