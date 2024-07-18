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

package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.KinesisAsyncClientProvider.TestKinesisAsyncClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Instant;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class KinesisAsyncStreamProxyTest {
    private static final SdkAsyncHttpClient ASYNC_HTTP_CLIENT = NettyNioAsyncHttpClient.create();

    private TestKinesisAsyncClient testKinesisAsyncClient;
    private KinesisAsyncStreamProxy kinesisAsyncStreamProxy;

    @BeforeEach
    public void setUp() {
        this.testKinesisAsyncClient = new TestKinesisAsyncClient();
        this.kinesisAsyncStreamProxy =
                new KinesisAsyncStreamProxy(testKinesisAsyncClient, ASYNC_HTTP_CLIENT);
    }

    @ParameterizedTest
    @MethodSource("provideStartingPosition")
    void testSubscribeToShard(
            StartingPosition inputStartingPosition,
            software.amazon.awssdk.services.kinesis.model.StartingPosition
                    expectedStartingPosition) {
        String consumerArn = STREAM_ARN + "/consumer/testing-consumer";
        String shardId = generateShardId(1);

        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder().subscriber(event -> {}).build();
        SubscribeToShardRequest expectedRequest =
                SubscribeToShardRequest.builder()
                        .consumerARN(consumerArn)
                        .shardId(shardId)
                        .startingPosition(expectedStartingPosition)
                        .build();

        testKinesisAsyncClient.setSubscribeToShardRequestValidator(
                (request, handler) -> {
                    assertThat(request).isEqualTo(expectedRequest);
                    assertThat(handler).isEqualTo(responseHandler);
                });

        kinesisAsyncStreamProxy.subscribeToShard(
                consumerArn, shardId, inputStartingPosition, responseHandler);
    }

    private static Stream<Arguments> provideStartingPosition() {
        Instant timestamp = Instant.ofEpochMilli(1721315064123L);
        String sequenceNumber = "sequence-number";
        return Stream.of(
                Arguments.of(
                        StartingPosition.fromStart(),
                        software.amazon.awssdk.services.kinesis.model.StartingPosition.builder()
                                .type(ShardIteratorType.TRIM_HORIZON)
                                .build()),
                Arguments.of(
                        StartingPosition.fromTimestamp(timestamp),
                        software.amazon.awssdk.services.kinesis.model.StartingPosition.builder()
                                .type(ShardIteratorType.AT_TIMESTAMP)
                                .timestamp(timestamp)
                                .build()),
                Arguments.of(
                        StartingPosition.continueFromSequenceNumber(sequenceNumber),
                        software.amazon.awssdk.services.kinesis.model.StartingPosition.builder()
                                .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .sequenceNumber(sequenceNumber)
                                .build()));
    }

    @Test
    void testCloseClosesKinesisClient() {
        assertThatNoException().isThrownBy(kinesisAsyncStreamProxy::close);
        assertThat(testKinesisAsyncClient.isClosed()).isTrue();
    }
}
