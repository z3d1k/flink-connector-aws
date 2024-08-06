/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.waiters.Waiter;
import software.amazon.awssdk.core.waiters.WaiterAcceptor;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.waiters.internal.WaitersRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Responsible for registering and de-registering EFO stream consumers. Will block until consumers
 * are ready.
 */
@Internal
public class StreamConsumerRegistrar {
    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerRegistrar.class);

    private final StreamProxy streamProxy;
    private final StreamConsumerWaiter waiter;

    public StreamConsumerRegistrar(final StreamProxy streamProxy, final Configuration configuration) {
        this.streamProxy = Preconditions.checkNotNull(streamProxy);
        this.waiter = new StreamConsumerWaiter(streamProxy, configuration);
    }

    /**
     * Register a stream consumer with the given name against the given stream. Blocks until the
     * consumer becomes active. If the stream consumer already exists, the ARN is returned.
     *
     * @param streamArn the ARN of stream to register the stream consumer against
     * @param streamConsumerName the name of the new stream consumer
     * @return the stream consumer ARN
     */
    public String registerStreamConsumer(final String streamArn, final String streamConsumerName) {
        LOG.debug("Registering stream consumer - {}::{}", streamArn, streamConsumerName);

        Optional<ConsumerDescription> consumerDescription =
                describeStreamConsumer(streamArn, streamConsumerName);

        if (!consumerDescription.isPresent()) {
            initiateStreamConsumerRegistration(streamArn, streamConsumerName);
        }

        WaiterResponse<ConsumerDescription> waiterResponse =
                waiter.waitUntilConsumerIsActive(streamArn, streamConsumerName);
        String streamConsumerArn =
                waiterResponse
                        .matched()
                        .response()
                        .map(ConsumerDescription::consumerARN)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "We should not be in this situation..."));

        LOG.debug("Using stream consumer - {}", streamConsumerArn);

        return streamConsumerArn;
    }

    /**
     * Deregister the stream consumer with the given ARN. Blocks until the consumer is deleted.
     *
     * @param streamConsumerArn the stream in which to deregister the consumer
     */
    public void deregisterStreamConsumer(final String streamConsumerArn) {
        LOG.debug("Deregistering stream consumer - {}", streamConsumerArn);

        Optional<ConsumerDescription> consumerDescription =
                describeStreamConsumer(streamConsumerArn);

        if (!consumerDescription.isPresent()) {
            // Consumer already deleted - nothing to do
            return;
        }

        boolean deleting =
                consumerDescription
                        .filter(
                                consumer ->
                                        ConsumerStatus.DELETING.equals(consumer.consumerStatus()))
                        .isPresent();

        if (!deleting) {
            initiateStreamConsumerDeregistration(streamConsumerArn);
        }

        waiter.waitUntilConsumerNotExists(streamConsumerArn);

        LOG.debug("Deregistered stream consumer - {}", streamConsumerArn);
    }

    private Optional<ConsumerDescription> describeStreamConsumer(
            final String streamArn, final String consumerName) {
        try {
            ConsumerDescription consumer =
                    streamProxy.describeStreamConsumer(streamArn, consumerName);
            return Optional.of(consumer);
        } catch (ResourceNotFoundException ex) {
            return Optional.empty();
        }
    }

    private Optional<ConsumerDescription> describeStreamConsumer(final String streamConsumerArn) {
        try {
            ConsumerDescription consumer = streamProxy.describeStreamConsumer(streamConsumerArn);
            return Optional.of(consumer);
        } catch (ResourceNotFoundException ex) {
            return Optional.empty();
        }
    }

    private void initiateStreamConsumerRegistration(
            final String streamArn, final String streamConsumerName) {
        try {
            streamProxy.registerStreamConsumer(streamArn, streamConsumerName);
        } catch (ResourceInUseException e) {
            // The stream consumer may have been created since we performed describe call
        }
    }

    private void initiateStreamConsumerDeregistration(final String streamConsumerArn) {
        try {
            streamProxy.deregisterStreamConsumer(streamConsumerArn);
        } catch (ResourceNotFoundException e) {
            // The stream consumer does not exist, nothing to do...
        }
    }

    /**
     * Implementation of AWS SDK {@link Waiter} used to waif for completion of operations on Kinesis
     * Stream Consumer.
     */
    @Internal
    static class StreamConsumerWaiter {
        private final Waiter<ConsumerDescription> streamConsumerActiveWaiter;
        private final Waiter<ConsumerDescription> streamConsumerNotExistsWaiter;

        private final StreamProxy streamProxy;

        public StreamConsumerWaiter(final StreamProxy streamProxy, final Configuration configuration) {
            this.streamProxy = streamProxy;

            this.streamConsumerActiveWaiter =
                    Waiter.builder(ConsumerDescription.class)
                            .acceptors(consumerExistsWaiterAcceptor())
                            .overrideConfiguration(
                                    waiterRegistrationOverrideConfiguration(configuration))
                            .build();
            this.streamConsumerNotExistsWaiter =
                    Waiter.builder(ConsumerDescription.class)
                            .acceptors(consumerNotExistsWaiterAcceptor())
                            .overrideConfiguration(
                                    waiterDeregistrationOverrideConfiguration(configuration))
                            .build();
        }

        public WaiterResponse<ConsumerDescription> waitUntilConsumerIsActive(
                final String streamArn, final String consumerName) {
            return streamConsumerActiveWaiter.run(
                    () -> streamProxy.describeStreamConsumer(streamArn, consumerName));
        }

        public WaiterResponse<ConsumerDescription> waitUntilConsumerNotExists(
                final String streamConsumerArn) {
            return streamConsumerNotExistsWaiter.run(
                    () -> streamProxy.describeStreamConsumer(streamConsumerArn));
        }

        private static List<WaiterAcceptor<? super ConsumerDescription>>
                consumerExistsWaiterAcceptor() {
            List<WaiterAcceptor<? super ConsumerDescription>> acceptors = new ArrayList<>();

            WaiterAcceptor<? super ConsumerDescription> consumerExistsAcceptor =
                    WaiterAcceptor.successOnResponseAcceptor(
                            response ->
                                    Objects.equals(
                                            response.consumerStatus(), ConsumerStatus.ACTIVE));

            acceptors.add(consumerExistsAcceptor);
            acceptors.addAll(WaitersRuntime.DEFAULT_ACCEPTORS);
            return acceptors;
        }

        private static List<WaiterAcceptor<? super ConsumerDescription>>
                consumerNotExistsWaiterAcceptor() {
            List<WaiterAcceptor<? super ConsumerDescription>> acceptors = new ArrayList<>();

            WaiterAcceptor<? super ConsumerDescription> consumerNotExistsAcceptor =
                    WaiterAcceptor.successOnExceptionAcceptor(
                            throwable -> {
                                String errorCode = null;
                                if (throwable instanceof AwsServiceException) {
                                    errorCode =
                                            ((AwsServiceException) throwable)
                                                    .awsErrorDetails()
                                                    .errorCode();
                                }
                                return Objects.equals(errorCode, "ResourceNotFoundException");
                            });

            acceptors.add(consumerNotExistsAcceptor);
            acceptors.addAll(WaitersRuntime.DEFAULT_ACCEPTORS);
            return acceptors;
        }

        private static WaiterOverrideConfiguration waiterRegistrationOverrideConfiguration(
                Configuration configuration) {
            return WaiterOverrideConfiguration.builder()
                    .backoffStrategy(FullJitterBackoffStrategy.builder()
                            .baseDelay(configuration.get(KinesisStreamsSourceConfigConstants.EFO_CONSUMER_REGISTRATION_INITIAL_BACKOFF))
                            .maxBackoffTime(configuration.get(KinesisStreamsSourceConfigConstants.EFO_CONSUMER_REGISTRATION_MAX_BACKOFF))
                            .build())
                    .waitTimeout(configuration.get(KinesisStreamsSourceConfigConstants.EFO_CONSUMER_REGISTRATION_TIMEOUT))
                    .build();
        }

        private static WaiterOverrideConfiguration waiterDeregistrationOverrideConfiguration(
                Configuration configuration) {
            return WaiterOverrideConfiguration.builder()
                    .backoffStrategy(FullJitterBackoffStrategy.builder()
                            .baseDelay(configuration.get(KinesisStreamsSourceConfigConstants.EFO_CONSUMER_REGISTRATION_INITIAL_BACKOFF))
                            .maxBackoffTime(configuration.get(KinesisStreamsSourceConfigConstants.EFO_CONSUMER_REGISTRATION_MAX_BACKOFF))
                            .build())
                    .waitTimeout(configuration.get(KinesisStreamsSourceConfigConstants.EFO_CONSUMER_REGISTRATION_TIMEOUT))
                    .build();
        }
    }
}
