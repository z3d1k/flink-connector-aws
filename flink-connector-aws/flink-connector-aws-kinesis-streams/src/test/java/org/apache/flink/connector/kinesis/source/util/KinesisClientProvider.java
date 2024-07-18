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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;

/** Provides {@link KinesisClient} with mocked Kinesis Stream behavior. */
public class KinesisClientProvider {

    /**
     * An implementation of the {@link KinesisClient} that allows control over Kinesis Service
     * responses.
     */
    public static class TestingKinesisClient implements KinesisClient {

        private Deque<ListShardItem> listShardQueue = new ArrayDeque<>();
        private Deque<String> shardIterators = new ArrayDeque<>();
        private Consumer<GetShardIteratorRequest> getShardIteratorValidation;
        private GetRecordsResponse getRecordsResponse;
        private Consumer<GetRecordsRequest> getRecordsValidation;
        private DescribeStreamSummaryResponse describeStreamSummaryResponse;
        private Consumer<DescribeStreamSummaryRequest> describeStreamSummaryRequestValidation;
        private boolean closed = false;

        // Describe stream consumer configuration
        private DescribeStreamConsumerResponse describeStreamConsumerResponse;
        private Consumer<DescribeStreamConsumerRequest> describeStreamConsumerValidation =
                request -> {};

        // Register stream consumer configuration
        private RegisterStreamConsumerResponse registerStreamConsumerResponse;
        private Consumer<RegisterStreamConsumerRequest> registerStreamConsumerValidation =
                request -> {};

        // Deregister stream consumer configuration
        private DeregisterStreamConsumerResponse deregisterStreamConsumerResponse;
        private Consumer<DeregisterStreamConsumerRequest> deregisterStreamConsumerValidation =
                request -> {};

        @Override
        public String serviceName() {
            return "kinesis";
        }

        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        public void setNextShardIterator(String shardIterator) {
            this.shardIterators.add(shardIterator);
        }

        public void setShardIteratorValidation(Consumer<GetShardIteratorRequest> validation) {
            this.getShardIteratorValidation = validation;
        }

        @Override
        public GetShardIteratorResponse getShardIterator(
                GetShardIteratorRequest getShardIteratorRequest)
                throws AwsServiceException, SdkClientException {
            getShardIteratorValidation.accept(getShardIteratorRequest);
            return GetShardIteratorResponse.builder().shardIterator(shardIterators.poll()).build();
        }

        public void setListShardsResponses(List<ListShardItem> items) {
            listShardQueue.addAll(items);
        }

        @Override
        public ListShardsResponse listShards(ListShardsRequest listShardsRequest)
                throws AwsServiceException, SdkClientException {
            ListShardItem item = listShardQueue.pop();

            item.validation.accept(listShardsRequest);
            return ListShardsResponse.builder()
                    .shards(item.shards)
                    .nextToken(item.nextToken)
                    .build();
        }

        public void setGetRecordsResponse(GetRecordsResponse getRecordsResponse) {
            this.getRecordsResponse = getRecordsResponse;
        }

        public void setGetRecordsValidation(Consumer<GetRecordsRequest> validation) {
            this.getRecordsValidation = validation;
        }

        @Override
        public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest)
                throws AwsServiceException, SdkClientException {
            getRecordsValidation.accept(getRecordsRequest);
            return getRecordsResponse;
        }

        public void setDescribeStreamSummaryResponse(
                DescribeStreamSummaryResponse describeStreamSummaryResponse) {
            this.describeStreamSummaryResponse = describeStreamSummaryResponse;
        }

        public void setDescribeStreamSummaryRequestValidation(
                Consumer<DescribeStreamSummaryRequest> describeStreamSummaryRequestValidation) {
            this.describeStreamSummaryRequestValidation = describeStreamSummaryRequestValidation;
        }

        @Override
        public DescribeStreamSummaryResponse describeStreamSummary(
                DescribeStreamSummaryRequest describeStreamSummaryRequest)
                throws AwsServiceException, SdkClientException {
            describeStreamSummaryRequestValidation.accept(describeStreamSummaryRequest);
            return describeStreamSummaryResponse;
        }

        @Override
        public KinesisServiceClientConfiguration serviceClientConfiguration() {
            // This is not used
            return null;
        }

        @Override
        public DescribeStreamConsumerResponse describeStreamConsumer(
                DescribeStreamConsumerRequest describeStreamConsumerRequest)
                throws AwsServiceException, SdkClientException {
            describeStreamConsumerValidation.accept(describeStreamConsumerRequest);
            return describeStreamConsumerResponse;
        }

        public void setDescribeStreamConsumerResponse(DescribeStreamConsumerResponse response) {
            this.describeStreamConsumerResponse = response;
        }

        public void setDescribeStreamConsumerValidation(
                Consumer<DescribeStreamConsumerRequest> describeStreamConsumerValidation) {
            this.describeStreamConsumerValidation = describeStreamConsumerValidation;
        }

        @Override
        public DeregisterStreamConsumerResponse deregisterStreamConsumer(
                DeregisterStreamConsumerRequest deregisterStreamConsumerRequest)
                throws AwsServiceException, SdkClientException {
            deregisterStreamConsumerValidation.accept(deregisterStreamConsumerRequest);
            return deregisterStreamConsumerResponse;
        }

        public void setDeregisterStreamConsumerResponse(DeregisterStreamConsumerResponse response) {
            this.deregisterStreamConsumerResponse = response;
        }

        public void setDeregisterStreamConsumerValidation(
                Consumer<DeregisterStreamConsumerRequest> deregisterStreamConsumerValidation) {
            this.deregisterStreamConsumerValidation = deregisterStreamConsumerValidation;
        }

        @Override
        public RegisterStreamConsumerResponse registerStreamConsumer(
                RegisterStreamConsumerRequest registerStreamConsumerRequest)
                throws AwsServiceException, SdkClientException {
            registerStreamConsumerValidation.accept(registerStreamConsumerRequest);
            return registerStreamConsumerResponse;
        }

        public void setRegisterStreamConsumerResponse(RegisterStreamConsumerResponse response) {
            this.registerStreamConsumerResponse = response;
        }

        public void setRegisterStreamConsumerValidation(
                Consumer<RegisterStreamConsumerRequest> registerStreamConsumerValidation) {
            this.registerStreamConsumerValidation = registerStreamConsumerValidation;
        }
    }

    /** Data class to provide a mocked response to ListShards() calls. */
    public static class ListShardItem {
        private final Consumer<ListShardsRequest> validation;
        private final List<Shard> shards;
        private final String nextToken;

        private ListShardItem(
                Consumer<ListShardsRequest> validation, List<Shard> shards, String nextToken) {
            this.validation = validation;
            this.shards = shards;
            this.nextToken = nextToken;
        }

        public static ListShardItem.Builder builder() {
            return new ListShardItem.Builder();
        }

        /** Builder for {@link ListShardItem}. */
        public static class Builder {
            private Consumer<ListShardsRequest> validation;
            private List<Shard> shards;
            private String nextToken;

            public Builder validation(Consumer<ListShardsRequest> validation) {
                this.validation = validation;
                return this;
            }

            public Builder shards(List<Shard> shards) {
                this.shards = shards;
                return this;
            }

            public Builder nextToken(String nextToken) {
                this.nextToken = nextToken;
                return this;
            }

            public KinesisClientProvider.ListShardItem build() {
                return new KinesisClientProvider.ListShardItem(validation, shards, nextToken);
            }
        }
    }
}
