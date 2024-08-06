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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/** Split fetcher manager implementation creating separate fetcher for each split. */
public class KinesisSourceFetcherManager extends SplitFetcherManager<Record, KinesisShardSplit> {
    private final Map<String, Integer> splitFetcherMapping = new HashMap<>();
    private final Map<Integer, Boolean> splitFetcherStatus = new HashMap<>();

    public KinesisSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Record>> elementsQueue,
            Supplier<SplitReader<Record, KinesisShardSplit>> splitReaderFactory,
            Configuration configuration) {
        super(elementsQueue, splitReaderFactory, configuration);
    }

    @Override
    public void addSplits(List<KinesisShardSplit> splitsToAdd) {
        for (KinesisShardSplit split : splitsToAdd) {
            SplitFetcher<Record, KinesisShardSplit> fetcher =
                    getOrCreateFetcher(split.splitId());
            fetcher.addSplits(Collections.singletonList(split));
            startFetcher(fetcher);
        }
    }

    @Override
    public void removeSplits(List<KinesisShardSplit> splitsToRemove) {
        for (KinesisShardSplit split : splitsToRemove) {
            Integer fetcherId = splitFetcherMapping.remove(split.splitId());
            if (fetcherId == null) {
                continue;
            }
            splitFetcherStatus.remove(fetcherId);

            SplitFetcher<Record, KinesisShardSplit> fetcher = fetchers.get(fetcherId);
            if (fetcher != null) {
                fetcher.shutdown();
            }
        }
    }

    @Override
    protected void startFetcher(SplitFetcher<Record, KinesisShardSplit> fetcher) {
        boolean isActive = splitFetcherStatus.getOrDefault(fetcher.fetcherId(), false);

        if (!isActive) {
            splitFetcherStatus.put(fetcher.fetcherId(), true);
            super.startFetcher(fetcher);
        }
    }

    private SplitFetcher<Record, KinesisShardSplit> getOrCreateFetcher(String splitId) {
        SplitFetcher<Record, KinesisShardSplit> fetcher;
        Integer fetcherId = splitFetcherMapping.get(splitId);

        if (fetcherId == null) {
            fetcher = createSplitFetcher();
        } else {
            fetcher = fetchers.get(fetcherId);
            // Fetcher has been stopped
            if (fetcher == null) {
                // remove fetcherId from tracking
                splitFetcherStatus.remove(fetcherId);
                fetcher = createSplitFetcher();
            }
        }

        splitFetcherMapping.put(splitId, fetcher.fetcherId());

        return fetcher;
    }
}
