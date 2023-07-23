package com.crm.test.SinkToJDBCBatch;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * SQL拼接类
 */
public class JoinSQLProcessV3 extends KeyedProcessFunction<String, Tuple5<String, String, String, String, String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(JoinSQLProcessV3.class);
   // private static String INSERT_OP_PRE = "insert into increment_all_table_options (`create_time`,`hour`,`op_type`,`table_name`) values";
   private String INSERT_OP_PRE = "";
    private ValueState<Integer> SQLNumValueState;
    private ValueState<String> lastOpTypeValueState;
    private ValueState<String> prefixValueState;
    private ValueState<StringBuffer> SQLValueState;
    private ValueState<StringBuffer> opSQLValueState;
    private ValueState<Long> timerValueState;


    public JoinSQLProcessV3() {
        INSERT_OP_PRE ="insert into increment_all_table_options(`create_time`,`hour`,`op_type`,`table_name`) values";
    }

    @Override
    public void processElement(Tuple5<String, String, String, String, String> tuple5, Context ctx, Collector<String> out) throws Exception {
        String tableName = tuple5.f0;
        String SQLPrefix = tuple5.f1;
        String SQLValue = tuple5.f2;
        String opSQLValue = tuple5.f3;
        String opType = tuple5.f4;

        if (lastOpTypeValueState.value() == null) {
            lastOpTypeValueState.update(opType);
        }
        if (SQLNumValueState.value() == null) {
            SQLNumValueState.update(new Integer(0));
        }

        if (prefixValueState.value() == null) {
            prefixValueState.update(SQLPrefix);
        }
        if (SQLValueState.value() == null || "initSQLValueState".equals(SQLValueState.value().toString())) {
            SQLValueState.update(new StringBuffer(SQLPrefix));
        }
        if (opSQLValueState.value() == null) {
            opSQLValueState.update(new StringBuffer(INSERT_OP_PRE));
        }

        switch (opType) {
            case "UD":
                //UD情况下，不记录tableOption
                collectSQL(ctx, out);
                out.collect(SQLPrefix + SQLValue);
                lastOpTypeValueState.update("UD");
                break;
            case "I":
            case "D":
                if (!SQLPrefix.equals(prefixValueState.value()) || !opType.equals(lastOpTypeValueState.value())) {
                    prefixValueState.update(SQLPrefix);
                    collectSQL(ctx, out);
                    lastOpTypeValueState.update(opType);
                    SQLValueState.update(new StringBuffer(SQLPrefix));
                }
                //注册1分钟定时器
                if (timerValueState.value() == null || timerValueState.value().equals(-1l)) {
                    long exeTime1 = System.currentTimeMillis() + 60 * 1000;
                    timerValueState.update(exeTime1);
                    ctx.timerService().registerProcessingTimeTimer(exeTime1);
                }

                //拼接SQL
                StringBuffer SQLValueStringBuffer = SQLValueState.value();
                StringBuffer opSqlStringBuffer = opSQLValueState.value();
                if ("I".equals(opType)) {
                    SQLValueStringBuffer.append(SQLValue).append(",");
                } else {
                    SQLValueStringBuffer.append(SQLValue).append(" OR ");
                }
                //拼接option表SQL
                opSqlStringBuffer.append(opSQLValue).append(",");

                //更新状态
                SQLValueState.update(SQLValueStringBuffer);
                opSQLValueState.update(opSqlStringBuffer);

                Integer num = SQLNumValueState.value();
                num++;
                //执行拼接sql Delete数据500条执行一次 Insert数据5000条执行一次
                if ((num >= 250 && "D".equals(opType)) || (num >= 9000 && "I".equals(opType))) {
                    collectSQL(ctx, out);
                    num = 0;
                }
                SQLNumValueState.update(num);
                break;
            default:
                LOG.warn("未知数据" + tableName + "--" + SQLPrefix + "--" + SQLValue);
        }
    }

    private void collectSQL(Context ctx, Collector<String> out) throws IOException, InterruptedException {
        String lastOpType = lastOpTypeValueState.value();
        StringBuffer SQLStringBuffer = SQLValueState.value();
        StringBuffer opSQLStringBuffe = opSQLValueState.value();
        if ("I".equals(lastOpType)) {
            if (SQLStringBuffer.toString().contains("values(")) {
                out.collect(SQLStringBuffer.deleteCharAt(SQLStringBuffer.length() - 1).toString());
            }
        } else if ("D".equals(lastOpType)) {
            if (SQLStringBuffer.toString().contains(" OR ")) {
                out.collect(SQLStringBuffer.delete(SQLStringBuffer.length() - 4, SQLStringBuffer.length() - 1).toString());
            }
        } else {
            LOG.info("lastOpTypeValueState is not I or D.It's " + lastOpType);
        }
        if (opSQLStringBuffe.toString().contains("values(")) {
            out.collect(opSQLStringBuffe.deleteCharAt(opSQLStringBuffe.length() - 1).toString());
        }
        SQLValueState.update(new StringBuffer("initSQLValueState"));
        opSQLValueState.update(new StringBuffer(INSERT_OP_PRE));
        SQLNumValueState.update(new Integer(0));
        //清除定时器
        if (timerValueState.value() != null && !timerValueState.value().equals(-1l)) {
            ctx.timerService().deleteProcessingTimeTimer(timerValueState.value());
            timerValueState.update(-1l);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        collectSQL(ctx, out);
        super.onTimer(timestamp, ctx, out);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SQLNumValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("SQLNum-key", Integer.class));
        prefixValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("prefixValueState-key", String.class));
        SQLValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("SQLValueState-key", StringBuffer.class));
        opSQLValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("opSQLValueState-key", StringBuffer.class));
        lastOpTypeValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastOpTypeValueState-key", String.class));
        timerValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("TimerValueState-key", Long.class));
    }


}
