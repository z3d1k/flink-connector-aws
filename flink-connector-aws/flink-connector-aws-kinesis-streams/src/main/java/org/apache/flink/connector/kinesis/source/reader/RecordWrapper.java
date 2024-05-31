package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.connector.kinesis.source.split.ChildShard;

import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.List;

/** Class representing either record or list of child shards as a end-marker for shard. */
public class RecordWrapper {
    private final Record record;
    private final List<ChildShard> childShards;

    private RecordWrapper(Record record, List<ChildShard> childShards) {
        this.record = record;
        this.childShards = childShards;
    }

    public boolean hasRecord() {
        return record != null;
    }

    public Record getRecord() {
        return record;
    }

    public List<ChildShard> getChildShards() {
        return childShards;
    }

    public static RecordWrapper record(Record record) {
        return new RecordWrapper(record, null);
    }

    public static RecordWrapper finishMarker(List<ChildShard> childShards) {
        return new RecordWrapper(null, childShards);
    }
}
