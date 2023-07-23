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
public class JoinSQLProcessV2 extends KeyedProcessFunction<String, Tuple5<String, String, String, String, String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(JoinSQLProcessV2.class);
    private static String INSERT_OP_PRE = "insert into increment_all_table_options (`create_time`,`hour`,`op_type`,`table_name`) values";
    private ValueState<Integer> SQLNumValueState;
    private ValueState<String> lastOpTypeValueState;
    private ValueState<String> insertPreValueState;
    private ValueState<String> deletePreValueState;
    private ValueState<StringBuffer> insertSQLValueState;
    private ValueState<StringBuffer> insertOpSQLValueState;
    private ValueState<StringBuffer> deleteSQLValueState;
    private ValueState<StringBuffer> deleteOpSQLValueState;
    private ValueState<Long> timerValueState;

    @Override
    public void processElement(Tuple5<String, String, String, String, String> tuple5, Context ctx, Collector<String> out) throws Exception {
        String tableName = tuple5.f0;
        String insertPre = tuple5.f1;
        String tableValue = tuple5.f2;
        String opValue = tuple5.f3;
        String opType = tuple5.f4;

        if (lastOpTypeValueState.value() == null) {
            lastOpTypeValueState.update(opType);
        }
        if (SQLNumValueState.value() == null) {
            SQLNumValueState.update(new Integer(0));
        }

        switch (opType) {
            case "UN":
                out.collect(tableValue);
                break;
            case "D":
                if (deletePreValueState.value() == null) {
                    deletePreValueState.update(insertPre);
                }
                if (deleteSQLValueState.value() == null) {
                    deleteSQLValueState.update(new StringBuffer(insertPre));
                }
                if (deleteOpSQLValueState.value() == null) {
                    deleteOpSQLValueState.update(new StringBuffer(INSERT_OP_PRE));
                }
                if (!opType.equals(lastOpTypeValueState.value())) {
                    collectSQL(ctx, out);
                    lastOpTypeValueState.update("D");
                }
                //注册1分钟定时器
                if (timerValueState.value() == null || timerValueState.value() == -1l) {
                    long exeTime1 = System.currentTimeMillis() + 60 * 1000;
                    timerValueState.update(exeTime1);
                    ctx.timerService().registerProcessingTimeTimer(exeTime1);
                }

                //拼接
                StringBuffer deleteSQLStringBuffer = deleteSQLValueState.value();
                StringBuffer deleteOpSqlStringBuffer = deleteOpSQLValueState.value();

                deleteSQLStringBuffer.append(tableValue).append(" OR ");
                deleteOpSqlStringBuffer.append(opValue).append(",");

                deleteSQLValueState.update(deleteSQLStringBuffer);
                deleteOpSQLValueState.update(deleteOpSqlStringBuffer);

                Integer num1 = SQLNumValueState.value();
                num1++;
                //5000条拼接一条
                if (num1 >= 500) {
                    collectSQL(ctx, out);
                    num1 = 0;
                }
                SQLNumValueState.update(num1);
                break;
            case "UD":
                collectSQL(ctx, out);
                out.collect(insertPre + tableValue);
                lastOpTypeValueState.update("UD");
                break;
            case "I":
            default:
                if (insertPreValueState.value() == null) {
                    insertPreValueState.update(insertPre);
                }
                if (insertSQLValueState.value() == null) {
                    insertSQLValueState.update(new StringBuffer(insertPre));
                }
                if (insertOpSQLValueState.value() == null) {
                    insertOpSQLValueState.update(new StringBuffer(INSERT_OP_PRE));
                }
                if (!insertPre.equals(insertPreValueState.value())) {
                    insertPreValueState.update(insertPre);
                    collectSQL(ctx, out);
                } else {
                    if (!opType.equals(lastOpTypeValueState.value())) {
                        collectSQL(ctx, out);
                        lastOpTypeValueState.update("I");
                    }
                    //注册1分钟的定时器
                    long exeTime = System.currentTimeMillis() + 60 * 1000;
                    timerValueState.update(exeTime);
                    ctx.timerService().registerProcessingTimeTimer(exeTime);

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
                        collectSQL(ctx, out);
                        num = 0;
                    }
                    SQLNumValueState.update(num);
                }
        }
    }

    private void collectSQL(Context ctx, Collector<String> out) throws IOException, InterruptedException {
        String lastOpType = lastOpTypeValueState.value();
        if ("I".equals(lastOpType)) {
            collectInsertSQL(ctx, out);
        } else if ("D".equals(lastOpType)) {
            collectDeleteSQL(ctx, out);
        } else {
            System.out.println("lastOpTypeValueState is not I or D.It's " + lastOpType);
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
        insertOpSQLValueState.update(new StringBuffer(INSERT_OP_PRE));
        SQLNumValueState.update(new Integer(0));
        if (timerValueState.value() != null && timerValueState.value() != -1l) {
            ctx.timerService().deleteProcessingTimeTimer(timerValueState.value());
        }
    }

    private void collectDeleteSQL(Context ctx, Collector<String> out) throws IOException, InterruptedException {
        StringBuffer deleteSQL = deleteSQLValueState.value();
        StringBuffer deleteOpSQL = deleteOpSQLValueState.value();

        if (deleteSQL.toString().contains(" OR ")) {
            out.collect(deleteSQL.delete(deleteSQL.length() - 4, deleteSQL.length() - 1).toString());
        }
        if (deleteOpSQL.toString().contains("values(")) {
            out.collect(deleteOpSQL.deleteCharAt(deleteOpSQL.length() - 1).toString());
        }
        deleteSQLValueState.update(new StringBuffer(deletePreValueState.value()));
        deleteOpSQLValueState.update(new StringBuffer(INSERT_OP_PRE));
        SQLNumValueState.update(new Integer(0));
        if (timerValueState.value() != null && timerValueState.value() != -1l) {
            ctx.timerService().deleteProcessingTimeTimer(timerValueState.value());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        collectSQL(ctx, out);
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
        deleteSQLValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("deleteSQLValueState-key", StringBuffer.class));
        deleteOpSQLValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("deleteOpSQLValueState-key", StringBuffer.class));
        lastOpTypeValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastOpTypeValueState-key", String.class));
        timerValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("TimerValueState-key", Long.class));
        deletePreValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("deletePreValueState-key", String.class));

    }


}
