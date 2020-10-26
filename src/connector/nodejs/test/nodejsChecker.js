const taos = require('../tdengine');

// establish connection
var conn = taos.connect({host: "192.168.1.59", user: "root", password: "taosdata", port: 6030});
var cursor = conn.cursor();
// create database
executeSql("create database if not exists jschecker", 0);
// use db
executeSql("use jschecker", 0);
// drop table
executeSql("drop table if exists jschecker.weather", 0);
// create table
executeSql("create table if not exists jschecker.weather(ts timestamp, temperature float, humidity int)", 0);
// insert
executeSql("insert into jschecker.weather (ts, temperature, humidity) values(now, 20.5, 34)", 1);
// select
executeQuery("select * from jschecker.weather");
// close connection
conn.close();

function executeQuery(sql) {
    var start = new Date().getTime();
    var promise = cursor.query(sql, true);
    var end = new Date().getTime();
    printSql(sql, promise != null, (end - start));
    promise.then(function (result) {
        result.pretty();
    });
}

function executeSql(sql, affectRows) {
    var start = new Date().getTime();
    var promise = cursor.execute(sql);
    var end = new Date().getTime();
    printSql(sql, promise == affectRows, (end - start));
}

function printSql(sql, succeed, cost) {
    console.log("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
}
