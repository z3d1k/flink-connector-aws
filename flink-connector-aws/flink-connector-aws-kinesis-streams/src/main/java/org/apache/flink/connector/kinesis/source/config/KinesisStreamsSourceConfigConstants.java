/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.config;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Constants to be used with the KinesisStreamsSource. */
@Experimental
public class KinesisStreamsSourceConfigConstants {
    /** Marks the initial position to use when reading from the Kinesis stream. */
    public enum InitialPosition {
        LATEST,
        TRIM_HORIZON,
        AT_TIMESTAMP
    }

    /** Defines mechanism used to consume records from Kinesis stream. */
    public enum ConsumerType {
        POLLING,
        EFO
    }

    public enum EfoConsumerRegistrationType {
        NONE
    }

    public static final ConfigOption<InitialPosition> STREAM_INITIAL_POSITION =
            ConfigOptions.key("flink.stream.initpos")
                    .enumType(InitialPosition.class)
                    .defaultValue(InitialPosition.LATEST)
                    .withDescription("The initial position to start reading Kinesis streams.");

    public static final ConfigOption<String> STREAM_INITIAL_TIMESTAMP =
            ConfigOptions.key("flink.stream.initpos.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The initial timestamp at which to start reading from the Kinesis stream. This is used when AT_TIMESTAMP is configured for the STREAM_INITIAL_POSITION.");

    public static final ConfigOption<String> STREAM_TIMESTAMP_DATE_FORMAT =
            ConfigOptions.key("flink.stream.initpos.timestamp.format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                    .withDescription(
                            "The date format used to parse the initial timestamp at which to start reading from the Kinesis stream. This is used when AT_TIMESTAMP is configured for the STREAM_INITIAL_POSITION.");

    public static final ConfigOption<Long> SHARD_DISCOVERY_INTERVAL_MILLIS =
            ConfigOptions.key("flink.shard.discovery.intervalmillis")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription("The interval between each attempt to discover new shards.");

    public static final ConfigOption<ConsumerType> CONSUMER_TYPE =
            ConfigOptions.key("type")
                    .enumType(ConsumerType.class)
                    .defaultValue(ConsumerType.POLLING);

    public static final ConfigOption<EfoConsumerRegistrationType> EFO_CONSUMER_REGISTRATION_TYPE =
            ConfigOptions.key("efo.consumer.registration.type")
                    .enumType(EfoConsumerRegistrationType.class)
                    .defaultValue(EfoConsumerRegistrationType.NONE);

    public static final ConfigOption<String> EFO_CONSUMER_NAME =
            ConfigOptions.key("efo.consumer.name")
                    .stringType()
                    .noDefaultValue();
    public static final ConfigOption<String> EFO_CONSUMER_ARN =
            ConfigOptions.key("efo.consumer.arn")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Duration> EFO_CONSUMER_REGISTRATION_INITIAL_BACKOFF =
            ConfigOptions.key("efo.consumer.registration.initial-backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5));
    public static final ConfigOption<Duration> EFO_CONSUMER_REGISTRATION_MAX_BACKOFF =
            ConfigOptions.key("efo.consumer.registration.max-backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Duration> EFO_CONSUMER_REGISTRATION_TIMEOUT =
            ConfigOptions.key("efo.consumer.registration.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3));

    public static final ConfigOption<Duration> EFO_CONSUMER_DEREGISTRATION_INITIAL_BACKOFF =
            ConfigOptions.key("efo.consumer.deregistration.initial-backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5));
    public static final ConfigOption<Duration> EFO_CONSUMER_DEREGISTRATION_MAX_BACKOFF =
            ConfigOptions.key("efo.consumer.deregistration.max-backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Duration> EFO_CONSUMER_DEREGISTRATION_TIMEOUT =
            ConfigOptions.key("efo.consumer.deregistration.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3));
}
