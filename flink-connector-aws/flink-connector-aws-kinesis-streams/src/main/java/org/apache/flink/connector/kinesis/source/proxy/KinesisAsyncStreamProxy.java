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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/** Implementation of async stream proxy for Kinesis client. */
@Internal
public class KinesisAsyncStreamProxy implements AsyncStreamProxy {
    private final KinesisAsyncClient kinesisAsyncClient;
    private final SdkAsyncHttpClient asyncHttpClient;

    public KinesisAsyncStreamProxy(
            KinesisAsyncClient kinesisAsyncClient, SdkAsyncHttpClient asyncHttpClient) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(
            String consumerArn,
            String shardId,
            StartingPosition startingPosition,
            SubscribeToShardResponseHandler responseHandler) {

        SubscribeToShardRequest request =
                SubscribeToShardRequest.builder()
                        .consumerARN(consumerArn)
                        .shardId(shardId)
                        .startingPosition(convertStartingPosition(startingPosition))
                        .build();
        return kinesisAsyncClient.subscribeToShard(request, responseHandler);
    }

    @Override
    public void close() throws IOException {
        kinesisAsyncClient.close();
        asyncHttpClient.close();
    }

    private software.amazon.awssdk.services.kinesis.model.StartingPosition convertStartingPosition(
            StartingPosition startingPosition) {
        software.amazon.awssdk.services.kinesis.model.StartingPosition.Builder builder =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder()
                        .type(startingPosition.getShardIteratorType());

        switch (startingPosition.getShardIteratorType()) {
            case LATEST:
            case TRIM_HORIZON:
                return builder.build();
            case AT_TIMESTAMP:
                Preconditions.checkArgument(
                        startingPosition.getStartingMarker() instanceof Instant,
                        "Invalid object given for SubscribeToShard when ShardIteratorType is AT_TIMESTAMP. Must be a Instant object.");
                return builder.timestamp((Instant) startingPosition.getStartingMarker()).build();
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                Preconditions.checkArgument(
                        startingPosition.getStartingMarker() instanceof String,
                        "Invalid object given for SubscribeToShard when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
                return builder.sequenceNumber((String) startingPosition.getStartingMarker())
                        .build();
        }

        throw new IllegalArgumentException(
                "Unsupported initial position configuration "
                        + startingPosition.getShardIteratorType());
    }
}
