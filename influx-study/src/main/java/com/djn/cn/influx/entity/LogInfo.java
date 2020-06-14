/**
 * Copyright (c) 2020-2025. op-platform.cn All Rights Reserved.
 * 项目名称：开放开发平台
 * 类名称：LogInfo.java
 * 创建人：op.nie-dongjia
 * 联系方式：niedongjiamail@qq.com
 * 开源地址: https://github.com/nie-dongjia
 * 项目官网:
 */
package com.djn.cn.influx.entity;

import lombok.Builder;
import lombok.Data;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

/**
 * <b>类  名：</b>com.djn.cn.influx.entity.LogInfo<br/>
 * <b>类描述：</b>TODO<br/>
 * <b>创建人：</b>op.nie-dongjia<br/>
 * <b>创建时间：</b>2020/6/14 16:18<br/>
 * <b>修改人：</b>op.nie-dongjia<br/>
 * <b>修改时间：</b>2020/6/14 16:18<br/>
 * <b>修改备注：</b><br/>
 *
 * @version 1.0.0 <br/>
 *
 */

@Builder
@Data
@Measurement(name = "logInfo")
public class LogInfo {
    @Column(name = "time")
    private String time;
    // 注解中添加tag = true,表示当前字段内容为tag内容
    @Column(name = "module", tag = true)
    private String module;
    @Column(name = "level", tag = true)
    private String level;
    @Column(name = "device_id", tag = true)
    private String deviceId;
    @Column(name = "msg")
    private String msg;
}
