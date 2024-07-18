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

package org.apache.flink.connector.kinesis.source.util;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/** Provides {@link KinesisAsyncClient} with mocked Kinesis Stream behavior. */
public class KinesisAsyncClientProvider {

    /**
     * An implementation of the {@link KinesisAsyncClient} that allows control over Kinesis Service
     * responses.
     */
    public static class TestKinesisAsyncClient implements KinesisAsyncClient {
        private boolean closed = false;

        private BiConsumer<SubscribeToShardRequest, SubscribeToShardResponseHandler>
                subscribeToShardRequestValidator;
        private CompletableFuture<Void> subscribeToShardResponse;

        public void setSubscribeToShardRequestValidator(
                BiConsumer<SubscribeToShardRequest, SubscribeToShardResponseHandler>
                        subscribeToShardRequestValidator) {
            this.subscribeToShardRequestValidator = subscribeToShardRequestValidator;
        }

        public void setSubscribeToShardResponse(CompletableFuture<Void> subscribeToShardResponse) {
            this.subscribeToShardResponse = subscribeToShardResponse;
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                SubscribeToShardRequest subscribeToShardRequest,
                SubscribeToShardResponseHandler asyncResponseHandler) {

            return subscribeToShardResponse;
        }

        @Override
        public String serviceName() {
            return "kinesis";
        }

        @Override
        public void close() {
            this.closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
