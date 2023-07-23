package com.crm.test.SinkToJDBCBatch;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonParseFlatMapV2 extends RichFlatMapFunction<String, Tuple5<String, String, String, String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessWindowFunction.class);
    private String errorMessageTable = "increment_error_message";
    private String unknowTypeTable = "increment_unknow_type";
    private String checkName;
    private String allDataTable;
    private String splitIndex = ";;;;;";

    public JsonParseFlatMapV2(String checkName, String allDataTable) {
        this.checkName = checkName;
        this.allDataTable = allDataTable;
    }

    @Override
    public void flatMap(String tuple, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
        List<Tuple5<String, String, String, String, String>> tuple5s = parseJsonToSQL(tuple);
        for (Tuple5<String, String, String, String, String> tuple5 : tuple5s) {
            out.collect(tuple5);
        }
    }

    /**
     * @param value 二元组，f0为topic简写，如Bzj_  f1为消息json串
     * @return
     */
    private List<Tuple5<String, String, String, String, String>> parseJsonToSQL(String value) {
        ;
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            String tableName = ("bz_" + jsonObject.getString("itfcode")).toLowerCase() + "_test1";
            Boolean isComplexJson = true;
            //测试是否为复杂json(头字段为数字)
            for (String key : jsonObject.keySet()) {
                if (!Character.isDigit(key.charAt(0))) {
                    isComplexJson = false;
                    break;
                }
                break;
            }
            if (isComplexJson) {
                ArrayList<Tuple5<String, String, String, String, String>> arrayList = new ArrayList<>();
                for (int i = 0; i < jsonObject.size(); i++) {
                    JSONObject tmp = JSONObject.parseObject((String) jsonObject.get(i));
                    tableName = ("bz_" + jsonObject.getString("itfcode")).toLowerCase() + "_test1";
                    arrayList.addAll(internalParseJsonToSQL(tableName, tmp));
                }
                return arrayList;
            } else {
                return internalParseJsonToSQL(tableName, jsonObject);
            }

//        } catch (JSONException e) {
//            e.printStackTrace();
//            ArrayList<Tuple5<String, String, String, String, String>> arrayList = new ArrayList<>();
//            arrayList.add(Tuple5.of(errorMessageTable, "", parseErrorMessage(tuple.f0 + "json解析异常", value), "", "UN"));
//            return arrayList;
        } catch (Exception e) {
            e.printStackTrace();
            ArrayList<Tuple5<String, String, String, String, String>> arrayList = new ArrayList<>();
            arrayList.add(parseErrorMessage("bz_test", "unknow", value, e.getMessage()));
//            arrayList.add(Tuple5.of(errorMessageTable, "", parseErrorMessage(tuple.f0, value), "", "UN"));
            return arrayList;
        }
    }

    private List<Tuple5<String, String, String, String, String>> internalParseJsonToSQL(String tableName, JSONObject jsonObject) throws InterruptedException {
        JSONObject key = jsonObject.getJSONObject("primary_key");
        String datatype = jsonObject.getString("datatype");
        String sendTime = jsonObject.getString("sendtime");
        Object dataLoads = jsonObject.get("dataload");
        ArrayList<Tuple5<String, String, String, String, String>> arrayList = new ArrayList<>();
        if (dataLoads != null && key != null) {
            String keyString = "";
            Integer keyNum = key.size();
            for (Map.Entry<String, Object> entry : key.entrySet()) {
                String value = ((JSONObject) dataLoads).getString(entry.getKey());
                //浮点类型：matches("^[0-9]*[.]?[0-9]*$")
                if (value != null && !value.matches("^[0-9]*$")) {
                    keyString = keyString + value;
                }
            }
            if (keyString.getBytes(StandardCharsets.UTF_8).length + keyNum >= 127) {
                LOG.error("表{}主键字段超长,json:{}", jsonObject.toString());
//                arrayList.add(Tuple5.of(errorMessageTable, "", parseErrorMessage(tableName + "_主键字段超长", jsonObject.toString()), "", "UN"));
                arrayList.add(parseErrorMessage(tableName.split("_")[0], tableName, jsonObject.toString(), "主键字段超长"));
                return arrayList;
            }
        }
        switch (datatype) {
            case "R":
            case "I": {
                //对账表,datatype为I的情况
                if (dataLoads instanceof JSONArray
                        && ((JSONArray) dataLoads).getJSONObject(0).containsKey("starttime")
                        && ((JSONArray) dataLoads).getJSONObject(0).containsKey("endtime")
                        && ((JSONArray) dataLoads).getJSONObject(0).containsKey("datatype")
                        && ((JSONArray) dataLoads).getJSONObject(0).containsKey("msgcount")) {
                    return parseCheckTable((JSONArray) dataLoads, checkName, tableName);
                } else {
                    JSONObject dataload = jsonObject.getJSONObject("dataload");
                    Tuple2<String, String> tuple2 = parseInsert(tableName, dataload);
                    String opValue = parseTableOption(sendTime, tableName, "I");
                    //Insert消息
                    arrayList.add(Tuple5.of(tableName, tuple2.f0, tuple2.f1, opValue, "I"));
                    return arrayList;
                }
            }
            case "U":
                JSONObject dataload = jsonObject.getJSONObject("dataload");
                if (key != null && key.size() > 0) {
                    Tuple2<String, String> tuple2 = parseInsert(tableName, dataload);
                    String opValue = parseTableOption(sendTime, tableName, "U");
                    //Insert消息
                    arrayList.add(Tuple5.of(tableName, tuple2.f0, tuple2.f1, opValue, "I"));
                    return arrayList;
                } else {
                    //Upsert消息
                    JSONObject before = jsonObject.getJSONObject("BEFORE");
                    Tuple2<String, String> deleteTuple = parseDelete(before, tableName);
                    Tuple2<String, String> tuple2 = parseInsert(tableName, dataload);
//                    arrayList.add(Tuple5.of(tableName, "", parseDelete(before, tableName), "", "UD"));
                    arrayList.add(Tuple5.of(tableName, deleteTuple.f0, deleteTuple.f1, "", "UD"));
                    arrayList.add(Tuple5.of(tableName, tuple2.f0, tuple2.f1, parseTableOption(sendTime, tableName, "U"), "I"));
                    return arrayList;
                }
            case "D":
                //Delete消息
//                arrayList.add(Tuple5.of(tableName, "", parseDelete(jsonObject.getJSONObject("BEFORE"), tableName), parseTableOption(sendTime, tableName, "D"), "D"));
                JSONObject before = jsonObject.getJSONObject("BEFORE");
                Tuple2<String, String> tuple2;
                if (before != null) {
                    tuple2 = parseDelete(jsonObject.getJSONObject("BEFORE"), tableName);
                } else {
                    //D数据，但是删除数据在dataload的情况
                    tuple2 = parseDelete(jsonObject.getJSONObject("dataload"), tableName);
                }
                arrayList.add(Tuple5.of(tableName, tuple2.f0, tuple2.f1, parseTableOption(sendTime, tableName, "D"), "D"));
                return arrayList;
            case "RC":
            case "FC":
            case "real":
                //对账表,datatype为RC/FC/real的情况
                return parseCheckTable((JSONArray) dataLoads, checkName, tableName);
            default:
                //未知类型
                arrayList.add(parseUnknownType(tableName, jsonObject, datatype, sendTime));
                return arrayList;
        }
    }

    private Tuple2<String, String> parseInsert(String tableName, JSONObject jsonObject) {
        StringBuffer sql = new StringBuffer("INSERT INTO ")
                .append(tableName)
                .append(" (");
        StringBuffer value = new StringBuffer("(");
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            Object entryValue = entry.getValue();
            if (key != null) {
                sql.append("`").append(key.toLowerCase()).append("`").append(",");
                if (entryValue == null) {
                    value.append("null").append(",");
                } else {
                    String valueString = entryValue.toString();
                    if (valueString.getBytes().length > 65533) {
                        valueString = valueString.substring(0, 32766);
                    }
                    value.append("'")
                            .append(escapeStr(valueString))
                            .append("'")
                            .append(",");
                }
            }
        }
        sql.deleteCharAt(sql.length() - 1).append(") ").append(" values");
        value.deleteCharAt(value.length() - 1).append(")");
        return Tuple2.of(sql.toString(), value.toString());
    }

    /**
     * 生成表操作记录value
     *
     * @param sendTime
     * @param tableName
     * @param type
     * @return
     */
    public String parseTableOption(String sendTime, String tableName, String type) {
        Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
//        String dateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()).format(instant);
        String hour = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault()).format(instant);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("table_name", tableName);
        jsonObject.put("create_time", sendTime);
        jsonObject.put("op_type", type);
        jsonObject.put("hour", hour);
        return parseInsert(allDataTable, jsonObject).f1;
    }


    /**
     * Delete语句拼接
     *
     * @param jsonObject
     * @param tableName
     * @return
     */
    public Tuple2<String, String> parseDelete(JSONObject jsonObject, String tableName) {
        StringBuffer sql = new StringBuffer("DELETE FROM ")
                .append(tableName)
                .append(" WHERE ");
        StringBuffer value = new StringBuffer("(");
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            Object entryValue = entry.getValue();
            if (key != null && entryValue != null) {
                value.append("`")
                        .append(key.toLowerCase())
                        .append("`")
                        .append("=")
                        .append("'")
                        .append(escapeStr(entryValue.toString()))
                        .append("' AND ");
            }
        }
        return Tuple2.of(sql.toString(), value.delete(value.length() - 5, value.length() - 1).append(")").toString());
    }

    /**
     * 生成对账信息SQL
     *
     * @param dataLoads
     * @param checkTable
     * @param tableName
     * @return
     */
    public List<Tuple5<String, String, String, String, String>> parseCheckTable(JSONArray dataLoads, String checkTable, String tableName) {
        ArrayList<Tuple5<String, String, String, String, String>> tuple5s = new ArrayList<>();
        Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
        //获取时间分区 2023050609
        String hour = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault()).format(instant);
        for (int i = 0; i < dataLoads.size(); i++) {
            JSONObject dataLoad = dataLoads.getJSONObject(i);
            dataLoad.put("table_name", tableName);
            dataLoad.put("hour", hour);
            Tuple2<String, String> tuple2 = parseInsert(checkTable, dataLoad);
            tuple5s.add(Tuple5.of(checkTable, tuple2.f0, tuple2.f1, "", "I"));
        }
        return tuple5s;
    }

    /**
     * 生成未知操作类型记录SQL
     *
     * @param tableName
     * @param jsonObject
     * @return
     */
    public Tuple5<String, String, String, String, String> parseUnknownType(String tableName, JSONObject jsonObject, String datatype, String sendTime) {
        String day = getDay();
        JSONObject unknowJson = new JSONObject();
        unknowJson.put("table_name", tableName);
        unknowJson.put("create_time", sendTime);
        unknowJson.put("message", jsonObject.toString());
        unknowJson.put("datatype", datatype);
        unknowJson.put("day", day);
        Tuple2<String, String> tuple = parseInsert(unknowTypeTable, unknowJson);
        return Tuple5.of(unknowTypeTable, tuple.f0, tuple.f1, "", "I");
    }

    /**
     * 生成Error记录SQL
     *
     * @param topicInfo
     * @param message
     * @return
     */
    public Tuple5<String, String, String, String, String> parseErrorMessage(String topicInfo, String tableName, String message, String errorInfo) {
        String day = getDay();
        JSONObject errorJson = new JSONObject();
        errorJson.put("topic_info", topicInfo);
        errorJson.put("table_name", tableName);
        errorJson.put("message", message);
        errorJson.put("error_info", errorInfo);
        errorJson.put("day", day);
        Tuple2<String, String> tuple = parseInsert(errorMessageTable, errorJson);
        return Tuple5.of(errorMessageTable, tuple.f0, tuple.f1, "", "I");
    }

    private String getDay() {
        Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
        //获取天分区
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(instant);
    }

    /**
     * 字符串转义规则
     *
     * @param str
     * @return
     */
    private String escapeStr(String str) {
        //去除最后一个反斜杠，防止sql中的单引号被转义
        if (str.endsWith("\\")) {
            char[] items = str.toCharArray();
            items[items.length - 1] = '/';
            str = String.valueOf(items);
        }
        //转义字符串
        str = str
                .replaceAll("'", "\"")
//                .replaceAll("\r", " ")
//                .replaceAll("\n", " ")
//                .replaceAll("\t", " ")
        ;
        return str;
    }

}
