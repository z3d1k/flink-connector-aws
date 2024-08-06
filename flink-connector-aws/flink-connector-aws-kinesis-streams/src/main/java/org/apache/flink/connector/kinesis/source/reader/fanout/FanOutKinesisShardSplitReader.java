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

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.KinesisAsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisShardSplitReaderBase;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** An implementation of the SplitReader. */
@Internal
public class FanOutKinesisShardSplitReader extends KinesisShardSplitReaderBase {
    private final KinesisAsyncStreamProxy asyncStreamProxy;
    private final String consumerArn;

    private final Map<String, FanOutKinesisShardSubscription> splitSubscriptions = new HashMap<>();

    public FanOutKinesisShardSplitReader(
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            KinesisAsyncStreamProxy asyncStreamProxy,
            String consumerArn) {
        super(shardMetricGroupMap);
        this.asyncStreamProxy = asyncStreamProxy;
        this.consumerArn = consumerArn;
    }

    @Override
    protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
        FanOutKinesisShardSubscription subscription = splitSubscriptions.get(splitState.getSplitId());

        FanOutKinesisSubscriptionEvent fanOutKinesisSubscriptionEvent = subscription.nextEvent();
        if (fanOutKinesisSubscriptionEvent != null) {
            SubscribeToShardEvent event;
            try {
                event = fanOutKinesisSubscriptionEvent.getEventOrThrow();
                boolean shardCompleted = event.continuationSequenceNumber() == null;
                if (shardCompleted) {
                    splitSubscriptions.remove(splitState.getSplitId());
                }
                return new RecordBatch(event.records(), event.millisBehindLatest(), shardCompleted);

            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    @Override
    protected void addSplitInternal(KinesisShardSplit split) {
        splitSubscriptions.put(
                split.splitId(),
                new FanOutKinesisShardSubscription(
                        asyncStreamProxy,
                        consumerArn,
                        split.getShardId(),
                        split.getStartingPosition()));
    }

    @Override
    public void close() throws Exception {
        splitSubscriptions.values()
                .forEach(FanOutKinesisShardSubscription::close);
        asyncStreamProxy.close();
    }
}
