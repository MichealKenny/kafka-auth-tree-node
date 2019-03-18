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
 * A node that sends authentication events to a Kafka topic.
 */


package org.forgerock.openam.auth.nodes;

import com.google.inject.assistedinject.Assisted;
import com.sun.identity.sm.RequiredValueValidator;
import com.iplanet.sso.SSOException;
import com.sun.identity.idm.AMIdentity;
import com.sun.identity.idm.IdRepoException;
import com.sun.identity.shared.debug.Debug;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import org.forgerock.openam.auth.node.api.*;
import org.forgerock.openam.auth.node.api.SingleOutcomeNode;
import org.forgerock.json.JsonValue;
import org.forgerock.openam.annotations.sm.Attribute;

import javax.inject.Inject;

import java.util.Set;
import java.util.Properties;
import java.util.Optional;
import java.util.Collections;
import java.util.Map;

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
        configClass = KafkaProducerNode.Config.class)
public class KafkaProducerNode extends SingleOutcomeNode {

    public interface Config {
        @Attribute(order = 10)
        default String kafkaBroker() {
            return "http://localhost:9092";
        }

        @Attribute(order = 20)
        default String kafkaTopic() {
            return "identity";
        }

        // @Attribute(order = 30, validators = {RequiredValueValidator.class})
        // default boolean outputJSON() {
        //     return false;
        // }

        @Attribute(order = 40)
        default String authResult() {
            return "Successful Authentication";
        }

        // @Attribute(order = 50)
        // Optional<String> eventVariable();

        // @Attribute(order = 60, validators = {RequiredValueValidator.class})
        // default boolean isTLS() {
        //     return false;
        // }

        // @Attribute(order = 70)
        // Optional<String> tlsCert();
    }

    private final Config config;
    private final static String DEBUG_FILE = "KafkaProducerNode";
    protected Debug debug = Debug.getInstance(DEBUG_FILE);

    /**
     * Guice constructor.
     * @param config The node configuration.
     * @throws NodeProcessException If there is an error reading the configuration.
     */
    @Inject
    public KafkaProducerNode(@Assisted Config config) throws NodeProcessException {
        this.config = config;
    }

    @Override
    public Action process(TreeContext context) throws NodeProcessException {
        //properties for producer
        Properties props = new Properties();
        props.put("bootstrap.servers", config.kafkaBroker());
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 
        //create producer
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
 
        //send messages to my-topic
        ProducerRecord producerRecord = new ProducerRecord<Integer, String>(config.kafkaTopic(), 
            "ForgeRock Identity Platform," + context.sharedState.get(USERNAME).asString() + "," + config.authResult());

        producer.send(producerRecord);
 
        //close producer
        producer.close();
        return goToNext().build();
    }
}