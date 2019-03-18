/*
 * The contents of this file are subject to the terms of the Common Development and
 * Distribution License (the License). You may not use this file except in compliance with the
 * License.
 *
 * You can obtain a copy of the License at legal/CDDLv1.0.txt. See the License for the
 * specific language governing permission and limitations under the License.
 *
 * When distributing Covered Software, include this CDDL Header Notice in each file and include
 * the License file at legal/CDDLv1.0.txt. If applicable, add the following below the CDDL
 * Header, with the fields enclosed by brackets [] replaced by your own identifying
 * information: "Portions copyright [year] [name of copyright owner]".
 *
 * Copyright 2019 ForgeRock.
 */
/**
 * micheal.kenny@forgerock.com
 *
 * NOT YET IMPLEMENTED - A node that receives events from a Kafka topic.
 */


package org.forgerock.openam.auth.nodes;

import com.google.inject.assistedinject.Assisted;
import com.sun.identity.sm.RequiredValueValidator;
import com.iplanet.sso.SSOException;
import com.sun.identity.idm.AMIdentity;
import com.sun.identity.idm.IdRepoException;
import com.sun.identity.shared.debug.Debug;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.forgerock.openam.auth.node.api.*;
import org.forgerock.openam.auth.node.api.SingleOutcomeNode;
import org.forgerock.json.JsonValue;
import org.forgerock.openam.annotations.sm.Attribute;

import javax.inject.Inject;

import java.util.Set;
import java.util.Properties;
import java.util.Arrays;
import java.util.Optional;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;

import static org.forgerock.openam.auth.node.api.SharedStateConstants.REALM;
import static org.forgerock.openam.auth.node.api.SharedStateConstants.USERNAME;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;


import javax.inject.Inject;


@Node.Metadata(outcomeProvider = SingleOutcomeNode.OutcomeProvider.class,
        configClass = KafkaConsumerNode.Config.class)
public class KafkaConsumerNode extends SingleOutcomeNode {

    public interface Config {
        @Attribute(order = 10)
        default String kafkaBroker() {
            return "http://localhost:9092";
        }

        @Attribute(order = 20)
        default String kafkaTopic() {
            return "identity";
        }

        @Attribute(order = 30, validators = {RequiredValueValidator.class})
        default String kafkaSharedStateVar() {
            return "kafkaRecords";
        }

        @Attribute(order = 40)
        default int historyLength() {
            return 20;
        }

        @Attribute(order = 50, validators = {RequiredValueValidator.class})
        default boolean isTLS() {
            return false;
        }

        @Attribute(order = 60)
        Optional<String> tlsCert();
    }

    private final Config config;
    private final static String DEBUG_FILE = "KafkaConsumerNode";
    protected Debug debug = Debug.getInstance(DEBUG_FILE);

    /**
     * Guice constructor.
     * @param config The node configuration.
     * @throws NodeProcessException If there is an error reading the configuration.
     */
    @Inject
    public KafkaConsumerNode(@Assisted Config config) throws NodeProcessException {
        this.config = config;
    }

    @Override
    public Action process(TreeContext context) throws NodeProcessException {
        //properties for consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", config.kafkaBroker());
        props.put("group.id", "forgerock-" + UUID.randomUUID().toString());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(config.kafkaTopic()));
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
        ConsumerRecords<String, String> records = consumer.poll(0);

        ArrayList<String> recordsList = new ArrayList<String>();

        for (ConsumerRecord<String, String> record : records)
        {
            recordsList.add(record.value());
        }

        consumer.close();

        JsonValue copyState = context.sharedState.copy().put(config.kafkaSharedStateVar(), recordsList);
        
        return goToNext().replaceSharedState(copyState).build();
    }
}