/**
 * Copyright (c) 2020-2025. op-platform.cn All Rights Reserved.
 * 项目名称：开放开发平台
 * 类名称：InfluxDbUtil.java
 * 创建人：op.nie-dongjia
 * 联系方式：niedongjiamail@qq.com
 * 开源地址: https://github.com/nie-dongjia
 * 项目官网:
 */
package com.djn.cn.influx.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <b>类  名：</b>com.djn.cn.influx.util.InfluxDbUtil<br/>
 * <b>类描述：</b>TODO<br/>
 * <b>创建人：</b>op.nie-dongjia<br/>
 * <b>创建时间：</b>2020/6/14 21:50<br/>
 * <b>修改人：</b>op.nie-dongjia<br/>
 * <b>修改时间：</b>2020/6/14 21:50<br/>
 * <b>修改备注：</b><br/>
 *
 * @version 1.0.0 <br/>
 *
 */
@Data
public class InfluxDbUtil {
    protected Logger logger = LoggerFactory.getLogger(InfluxDbUtil.class);

    private String userName;
    private String password;
    private String url;
    public String database;
    private String retentionPolicy;
    // InfluxDB实例
    private InfluxDB influxDB;

    // 数据保存策略
    public static String policyNamePix = "logRetentionPolicy_";

    public InfluxDbUtil(String userName, String password, String url, String database,
                         String retentionPolicy) {
        this.userName = userName;
        this.password = password;
        this.url = url;
        this.database = database;
        this.retentionPolicy = retentionPolicy == null || "".equals(retentionPolicy) ? "autogen" : retentionPolicy;
        this.influxDB = influxDbBuild();
    }

    /**
     * 连接数据库 ，若不存在则创建
     *
     * @return influxDb实例
     */
    private InfluxDB influxDbBuild() {
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(url, userName, password);
        }
        try {
            createDB(database);
            influxDB.setDatabase(database);
        } catch (Exception e) {
            logger.error("create influx db failed, error: {}", e.getMessage());
        } finally {
            influxDB.setRetentionPolicy(retentionPolicy);
        }
        influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
        return influxDB;
    }
    private void createDB(String database) {
        influxDB.query(new Query("CREATE DATABASE " + database));
    }

    /**
     * 查询
     *
     * @param command 查询语句
     * @return
     */
    public <T> List<T> query(String command, Class<T> clazz) {
        logger.info("query command:{}",command);
        JSONArray resultArr = new JSONArray();
        QueryResult query = influxDB.query(new Query(command, database));
        for (QueryResult.Result result : query.getResults()) {
            List<QueryResult.Series> series = result.getSeries();
            if(series==null){
                continue;
            }
            for (QueryResult.Series serie : series) {
                List<List<Object>> values = serie.getValues();
                List<String> colums = serie.getColumns();
                Map<String, String> tags = serie.getTags();

                // 封装查询结果
                for (int i=0;i<values.size();++i){
                    JSONObject jsonData = new JSONObject();
                    if (tags!=null && tags.keySet().size()>0){
                        tags.forEach((k,v)-> jsonData.put(k,v));
                    }
                    for (int j=0;j<colums.size();++j){
                        jsonData.put(colums.get(j),values.get(i).get(j));
                    }
                    resultArr.add(jsonData);
                }
            }
        }
        return JSONObject.parseArray(resultArr.toJSONString(), clazz);
    }

    /**
     * 插入
     *
     * @param measurement 表
     * @param tags 标签
     * @param fields 字段
     */
    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields, long time, TimeUnit timeUnit) {
        Point.Builder builder = Point.measurement(measurement).tag(tags).fields(fields).time(time, timeUnit);
        influxDB.write(database, retentionPolicy, builder.build());
    }

    /**
     * 插入
     *
     * @param point
     */
    public void insert(Point point) {
        if (null==point){
            return;
        }
        influxDB.write(database, retentionPolicy, point);
    }

    /**
     * 批量写入数据
     *
     * @param points
     */
    public void batchInsert(List<Point> points) {
        if (points==null || points.size()<=0){
            logger.info("数据为空");
            return;
        }
        BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy(retentionPolicy).build();
        for(Point p:points){
            batchPoints.point(p);
        }
        influxDB.write(batchPoints);
    }

    /**
     * 删除表
     *
     * @param measurement
     * @return 返回错误信息
     */
    public void deleteMeasurement(String measurement) {
        logger.info("dropMeasurement measurement:{}",measurement);
        influxDB.query(new Query("drop measurement \"" + measurement + "\"", database));
    }

    /**
     * 删除数据库
     */
    public void deleteDb() {
        influxDB.query(new Query("DROP DATABASE \"" + database + "\""));
    }

    /**
     * 测试连接是否正常
     *
     * @return true 正常
     */
    public boolean ping() {
        Pong pong = influxDB.ping();
        if (pong != null) {
            return true;
        }
        return false;
    }

    /**
     * 关闭数据库
     */
    public void close() {
        influxDB.close();
    }

}
