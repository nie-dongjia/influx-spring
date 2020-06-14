/**
 * Copyright (c) 2020-2025. op-platform.cn All Rights Reserved.
 * 项目名称：开放开发平台
 * 类名称：WebApplication.java
 * 创建人：op.nie-dongjia
 * 联系方式：niedongjiamail@qq.com
 * 开源地址: https://github.com/nie-dongjia
 * 项目官网:
 */
package com.djn.cn.influx;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <b>类  名：</b>com.djn.cn.influx.WebApplication<br/>
 * <b>类描述：</b>TODO<br/>
 * <b>创建人：</b>op.nie-dongjia<br/>
 * <b>创建时间：</b>2020/6/14 15:36<br/>
 * <b>修改人：</b>op.nie-dongjia<br/>
 * <b>修改时间：</b>2020/6/14 15:36<br/>
 * <b>修改备注：</b><br/>
 *
 * @version 1.0.0 <br/>
 *
 */
@SpringBootApplication // same as @Configuration @EnableAutoConfiguration @ComponentScan
public class WebApplication implements CommandLineRunner {
    public static void main(final String[] args) throws Exception {
        SpringApplication.run(WebApplication.class, args);
    }
    @Autowired
    private InfluxDB influxDB;

    @Override
    public void run(String... args) throws Exception {
        influxDB.setDatabase("DB_TestDB");
        influxDB.write(
                Point.measurement("demo_api").tag("name", "hello")
                        .addField("rt", 10).addField("times", 320)
                        .build()
        );
        // 普通查询
        QueryResult rs = influxDB.query(new Query("select * from demo_api", "DB_TestDB"));

        System.out.println("query result => "+rs);
        if (!rs.hasError() && !rs.getResults().isEmpty()) {
            rs.getResults().forEach(System.out::println);
        }
    }
}
