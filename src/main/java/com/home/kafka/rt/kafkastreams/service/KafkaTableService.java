package com.home.kafka.rt.kafkastreams.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Service;

@Service
@EnableKafkaStreams
public class KafkaTableService {

    @Bean
    public KTable<String, String> kTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.table("transaction-limits", Materialized.with(Serdes.String(), Serdes.String()));
    }
}
