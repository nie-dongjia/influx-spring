/**
 * Copyright (c) 2020-2025. op-platform.cn All Rights Reserved.
 * 项目名称：开放开发平台
 * 类名称：InfluxDbConfig.java
 * 创建人：op.nie-dongjia
 * 联系方式：niedongjiamail@qq.com
 * 开源地址: https://github.com/nie-dongjia
 * 项目官网:
 */
package com.djn.cn.influx.config;

import com.djn.cn.influx.util.InfluxDbUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <b>类  名：</b>com.djn.cn.influx.config.InfluxDbConfig<br/>
 * <b>类描述：</b>TODO<br/>
 * <b>创建人：</b>op.nie-dongjia<br/>
 * <b>创建时间：</b>2020/6/14 15:21<br/>
 * <b>修改人：</b>op.nie-dongjia<br/>
 * <b>修改时间：</b>2020/6/14 15:21<br/>
 * <b>修改备注：</b><br/>
 *
 * @version 1.0.0 <br/>
 *
 */
@Configuration
public class InfluxDbConfig {

    @Value("${spring.influx.url:''}")
    private String influxDBUrl;

    @Value("${spring.influx.user:''}")
    private String userName;

    @Value("${spring.influx.password:''}")
    private String password;

    @Value("${spring.influx.database:''}")
    private String database;

    @Bean
    public InfluxDbUtil influxDbUtils() {
        return new InfluxDbUtil(userName, password, influxDBUrl, database, "");
    }
}
