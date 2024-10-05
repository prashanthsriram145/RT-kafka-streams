package com.home.kafka.rt.kafkastreams.service;

import com.home.kafka.rt.kafkastreams.model.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
@EnableKafkaStreams
public class KafkaStreamsService {

    private final KTable<String, String> ktable;

    public KafkaStreamsService(KTable<String, String> kTable) {
        this.ktable = kTable;
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        // Define the input Kafka topic to consume from
        KStream<String, String> transactionsStream = streamsBuilder.stream("transactions");
        KStream<String, String> transformedStream = transactionsStream.map((key, value) -> {
            String newKey = Objects.requireNonNull(Transaction.fromJson(value)).getLocation();
            return KeyValue.pair(newKey, value);
        });

        KStream<String, String> joinedStream = transformedStream.join(ktable,
                (transaction, locationLimit) -> Transaction.fromJson(transaction).getAmount() > Double.parseDouble(locationLimit)
                        ? transaction : null,
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        KStream<String, String> filteredSteam = joinedStream.filter((location, transactionId) -> transactionId != null);


        filteredSteam.to("suspicious-transactions", Produced.with(Serdes.String(), Serdes.String()));

        // Predicate to detect suspicious transactions (e.g., amount > 10,000)
//        Predicate<String, String> isSuspicious = (key, transactionJson) -> {
//            // Parse the transaction JSON
//            Transaction transaction = Transaction.fromJson(transactionJson);
//            assert transaction != null;
//            return transaction.getAmount() > 10000; // Condition for flagging
//        };
//
//        // Split the stream into suspicious and non-suspicious transactions
//        KStream<String, String>[] branches = transactionsStream.branch(
//                isSuspicious,
//                (key, transactionJson) -> true // Non-suspicious transactions
//        );
//
//        // Send suspicious transactions to the output Kafka topic
//        branches[0].to("suspicious-transactions", Produced.with(Serdes.String(), Serdes.String()));

        return transactionsStream;
    }


}
