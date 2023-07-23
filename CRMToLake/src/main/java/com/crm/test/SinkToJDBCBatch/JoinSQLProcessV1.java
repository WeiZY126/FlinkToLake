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
public class JoinSQLProcessV1 extends KeyedProcessFunction<String, Tuple5<String, String, String, String, String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(JoinSQLProcessV1.class);
    private ValueState<Integer> SQLNumValueState;
    private ValueState<String> insertPreValueState;
    private ValueState<StringBuffer> insertSQLValueState;
    private ValueState<StringBuffer> insertOpSQLValueState;
    private ValueState<Long> timerValueState;

    @Override
    public void processElement(Tuple5<String, String, String, String, String> tuple5, Context ctx, Collector<String> out) throws Exception {
        String tableName = tuple5.f0;
        String insertPre = tuple5.f1;
        String tableValue = tuple5.f2;
        String opValue = tuple5.f3;
        String opType = tuple5.f4;
        if (insertOpSQLValueState.value() == null) {
            insertOpSQLValueState.update(new StringBuffer("insert into increment_all_table_options (`create_time`,`hour`,`op_type`,`table_name`) values"));
        }
        if (SQLNumValueState.value() == null) {
            SQLNumValueState.update(new Integer(0));
        }
        if (insertPreValueState.value() == null) {
            insertPreValueState.update(insertPre);
        }
        if (insertSQLValueState.value() == null) {
            insertSQLValueState.update(new StringBuffer(insertPre));
        }
        switch (opType) {
            case "UN":
                out.collect(tableValue);
                break;
            case "D":
                collectInsertSQL(ctx, out);
                out.collect(tableValue);
                out.collect("insert into increment_all_table_options (`create_time`,`hour`,`op_type`,`table_name`) values" + opValue);
                break;
            case "UD":
                collectInsertSQL(ctx, out);
                out.collect(tableValue);
                break;
            case "I":
            default:
                if (!insertPre.equals(insertPreValueState.value())) {
                    insertPreValueState.update(insertPre);
                    collectInsertSQL(ctx, out);
                } else {
                    if (timerValueState.value() == null || timerValueState.value() == -1l) {
                        //注册1分钟的定时器
                        long exeTime = System.currentTimeMillis() + 60 * 1000;
                        timerValueState.update(exeTime);
                        ctx.timerService().registerProcessingTimeTimer(exeTime);
                    }

                    //拼接
                    StringBuffer insertSQL = insertSQLValueState.value();
                    StringBuffer insertOpSQL = insertOpSQLValueState.value();
                    insertSQL.append(tableValue).append(",");
                    insertOpSQL.append(opValue).append(",");
                    insertSQLValueState.update(insertSQL);
                    insertOpSQLValueState.update(insertOpSQL);
                    Integer num = SQLNumValueState.value();
                    num++;
                    //5000条拼接一条
                    if (num >= 5000) {
                        collectInsertSQL(ctx, out);
                        num = 0;
                    }
                    SQLNumValueState.update(num);
                }
        }
    }

    private void collectInsertSQL(Context ctx, Collector<String> out) throws IOException {
        StringBuffer insertSQL = insertSQLValueState.value();
        StringBuffer insertOpSQL = insertOpSQLValueState.value();
        if (insertSQL.toString().contains("values(")) {
            out.collect(insertSQL.deleteCharAt(insertSQL.length() - 1).toString());
        }
        if (insertOpSQL.toString().contains("values(")) {
            out.collect(insertOpSQL.deleteCharAt(insertOpSQL.length() - 1).toString());
        }
        insertSQLValueState.update(new StringBuffer(insertPreValueState.value()));
        insertOpSQLValueState.update(new StringBuffer("insert into increment_all_table_options (`create_time`,`hour`,`op_type`,`table_name`) values"));
        SQLNumValueState.update(new Integer(0));
        if (timerValueState.value() != null && timerValueState.value() != -1l) {
            ctx.timerService().deleteProcessingTimeTimer(timerValueState.value());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        collectInsertSQL(ctx, out);
        timerValueState.update(-1l);
        super.onTimer(timestamp, ctx, out);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SQLNumValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("SQLNum-key", Integer.class));
        insertPreValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("insertPre-key", String.class));
        insertSQLValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("insertSQLValueState-key", StringBuffer.class));
        insertOpSQLValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("insertOpSQLValueState-key", StringBuffer.class));
        timerValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("TimerValueState-key", Long.class));
    }


}
