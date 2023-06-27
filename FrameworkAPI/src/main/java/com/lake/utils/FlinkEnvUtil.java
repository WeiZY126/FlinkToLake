package com.lake.utils;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvUtil {

    /**
     * 构建流式环境
     * @param check_path
     * @return
     */
    public static StreamExecutionEnvironment creatEnv5(String check_path) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启ck
        env.enableCheckpointing(20 * 1000L);
        //设置状态后端存储路径
        env.getCheckpointConfig().setCheckpointStorage(new FsStateBackend(check_path, false));
        //checkpoint最大失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(6);
        //相邻checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //checkpoint发送间隔
        env.getCheckpointConfig().setCheckpointInterval(20 * 1000L);
        //checkpoint模式，精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L);
        //最大同时存在的checkpoint数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //开启非对齐checkpoint
        env.getCheckpointConfig().setForceUnalignedCheckpoints(true);
        //checkpoint清除策略
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //采用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

}
