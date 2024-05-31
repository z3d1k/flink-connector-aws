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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.ChildShard;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * An implementation of the SplitReader that periodically polls the Kinesis stream to retrieve
 * records.
 */
@Internal
public class PollingKinesisShardSplitReader
        implements SplitReader<RecordWrapper, KinesisShardSplit> {

    private static final RecordsWithSplitIds<RecordWrapper> INCOMPLETE_SHARD_EMPTY_RECORDS =
            new KinesisRecordsWithSplitIds(Collections.emptyIterator(), null, false, null);

    private final StreamProxy kinesis;
    private final Deque<KinesisShardSplitState> assignedSplits = new ArrayDeque<>();

    public PollingKinesisShardSplitReader(StreamProxy kinesisProxy) {
        this.kinesis = kinesisProxy;
    }

    @Override
    public RecordsWithSplitIds<RecordWrapper> fetch() throws IOException {
        KinesisShardSplitState splitState = assignedSplits.poll();
        if (splitState == null) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        GetRecordsResponse getRecordsResponse =
                kinesis.getRecords(
                        splitState.getStreamArn(),
                        splitState.getShardId(),
                        splitState.getNextStartingPosition());
        boolean isComplete = getRecordsResponse.nextShardIterator() == null;
        List<ChildShard> childShards = mapChildShards(getRecordsResponse.childShards());

        if (hasNoRecords(getRecordsResponse)) {
            if (isComplete) {
                return new KinesisRecordsWithSplitIds(
                        Collections.emptyIterator(), splitState.getSplitId(), true, childShards);
            } else {
                assignedSplits.add(splitState);
                return INCOMPLETE_SHARD_EMPTY_RECORDS;
            }
        }

        splitState.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(
                        getRecordsResponse
                                .records()
                                .get(getRecordsResponse.records().size() - 1)
                                .sequenceNumber()));

        assignedSplits.add(splitState);

        return new KinesisRecordsWithSplitIds(
                getRecordsResponse.records().iterator(),
                splitState.getSplitId(),
                isComplete,
                childShards);
    }

    private List<ChildShard> mapChildShards(
            List<software.amazon.awssdk.services.kinesis.model.ChildShard> childShards) {
        if (childShards == null || childShards.isEmpty()) {
            return null;
        }
        ArrayList<ChildShard> result = new ArrayList<>();
        childShards.forEach(childShard -> result.add(new ChildShard(childShard)));
        return result;
    }

    private boolean hasNoRecords(GetRecordsResponse getRecordsResponse) {
        return !getRecordsResponse.hasRecords() || getRecordsResponse.records().isEmpty();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChanges) {
        for (KinesisShardSplit split : splitsChanges.splits()) {
            assignedSplits.add(new KinesisShardSplitState(split));
        }
    }

    @Override
    public void wakeUp() {
        // Do nothing because we don't have any sleep mechanism
    }

    @Override
    public void close() throws Exception {
        kinesis.close();
    }

    private static class KinesisRecordsWithSplitIds implements RecordsWithSplitIds<RecordWrapper> {

        private final Iterator<Record> recordsIterator;
        private final String splitId;
        private final boolean isComplete;
        private final List<ChildShard> childShards;

        private boolean finishMarkerSent = false;

        public KinesisRecordsWithSplitIds(
                Iterator<Record> recordsIterator,
                String splitId,
                boolean isComplete,
                List<ChildShard> childShards) {
            this.recordsIterator = recordsIterator;
            this.splitId = splitId;
            this.isComplete = isComplete;
            this.childShards = childShards;
        }

        @Nullable
        @Override
        public String nextSplit() {
            return recordsIterator.hasNext() || (isComplete && !finishMarkerSent) ? splitId : null;
        }

        @Nullable
        @Override
        public RecordWrapper nextRecordFromSplit() {
            if (recordsIterator.hasNext()) {
                return RecordWrapper.record(recordsIterator.next());
            }
            if (isComplete && !finishMarkerSent) {
                finishMarkerSent = true;
                return RecordWrapper.finishMarker(childShards);
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            if (splitId == null) {
                return Collections.emptySet();
            }
            if (recordsIterator.hasNext()) {
                return Collections.emptySet();
            }
            return isComplete ? singleton(splitId) : Collections.emptySet();
        }
    }
}
