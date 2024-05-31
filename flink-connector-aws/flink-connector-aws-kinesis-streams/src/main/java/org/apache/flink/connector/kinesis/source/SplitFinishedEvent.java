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

package org.apache.flink.connector.kinesis.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.connector.kinesis.source.split.ChildShard;

import java.util.List;

/** Source event to indicate that source reader finished reading split. */
public class SplitFinishedEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final List<ChildShard> childShards;

    public SplitFinishedEvent(String splitId, List<ChildShard> childShards) {
        this.splitId = splitId;
        this.childShards = childShards;
    }

    public String getSplitId() {
        return splitId;
    }

    public List<ChildShard> getChildShards() {
        return childShards;
    }

    @Override
    public String toString() {
        return "SplitFinishedEvent{splitId='" + splitId + "'}";
    }
}
