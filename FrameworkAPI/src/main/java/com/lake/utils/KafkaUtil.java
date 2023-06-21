package com.lake.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaUtil<T> implements Serializable {
    private ParameterTool parameterTool;

    /**
     * Description: 初始化配置文件
     */
    public KafkaUtil(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    public SourceStreamBuilder<T> source() {
        return new SourceStreamBuilder<T>();
    }

    public class SourceStreamBuilder<T> {
        private KafkaSource<T> kafkaSource;
        private KafkaSourceBuilder<T> sourceBuilder;

        /**
         * 初始化Source
         *
         * @return
         */
        private SourceStreamBuilder() {
            sourceBuilder = KafkaSource.builder();
            sourceBuilder
                    .setBootstrapServers(parameterTool.get("bootstrap.servers"))
                    .setGroupId(parameterTool.get("group.id"))
                    .setProperties(parameterTool.getProperties());

            initPartitionsAndTopics(sourceBuilder);

            String startingOffsets = parameterTool.get("kafka.consumer.startOffsets").toLowerCase();
            switch (startingOffsets) {
                case "earliest":
                    sourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                    break;
                case "latest":
                    sourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                    break;
                case "timestamp":
                    sourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(parameterTool.getLong("kafka.consumer.timestamp")));
                    break;
                case "committedoffsets":
                    sourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
                    break;
                case "offsets":
                default:
                    throw new UnsupportedOperationException(startingOffsets + " mode is not supported. Check startingOffsets is available");
            }
        }

        /**
         * 判断分区和topic信息
         *
         * @param builder
         */
        private void initPartitionsAndTopics(KafkaSourceBuilder<T> builder) {
            if (parameterTool.has("kakfa.partition.ids") && parameterTool.get("kakfa.partition.ids") != null && !parameterTool.get("kakfa.partition.ids").isEmpty()) {
                String[] partitionIds = parameterTool.get("kakfa.partition.ids").split(",");
                Set<TopicPartition> topicPartitionSet = Stream.of(partitionIds)
                        .map(partitionId -> new TopicPartition(partitionId.split(" ")[0], Integer.parseInt(partitionId.split(" ")[1])))
                        .collect(Collectors.toSet());
                builder.setPartitions(topicPartitionSet);
            } else {
                String[] topics = parameterTool.get("kakfa.topics.name").split(",");
                builder.setTopics(Arrays.asList(topics));
            }
        }

        public SourceStreamBuilder<T> buildWithDeserializer(DeserializationSchema deserializationSchema) {
            kafkaSource = sourceBuilder
                    .setValueOnlyDeserializer(deserializationSchema)
                    .build();
            return this;
        }

        public SingleOutputStreamOperator<T> getDataStream(StreamExecutionEnvironment env) {
            return env.fromSource(
                    this.kafkaSource,
                    WatermarkStrategy.noWatermarks(), "Kafka-Source");
        }
    }

    public class SinkStreamBuilder<T> {
        private KafkaSink<T> kafkaSink;

        private KafkaSink<String> initKafkaSink(JSONObject js) {
            KafkaRecordSerializationSchemaBuilder<String> stringSchemaBuilder = KafkaRecordSerializationSchema.builder()
                    .setTopic(js.getString("topic"))
                    .setValueSerializationSchema(new SimpleStringSchema());
            //判断是否分区
            if (js.containsKey("partitionId")) {
                stringSchemaBuilder.
                        setPartitioner(new FlinkKafkaPartitioner<String>() {
                            @Override
                            public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                                return js.getIntValue("partitionId");
                            }
                        });
            }
            return KafkaSink.<String>builder()
                    .setBootstrapServers(js.getString("bootstrapServers"))
                    .setRecordSerializer(stringSchemaBuilder.build())
                    .build();
        }


        public void setSinkTo(DataStream<T> stream) {
            stream.sinkTo(this.kafkaSink).name("kafka-org.apache.iceberg.flink.sink");
        }
    }


}