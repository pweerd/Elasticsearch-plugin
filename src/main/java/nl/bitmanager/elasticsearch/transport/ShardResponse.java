/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.bitmanager.elasticsearch.transport;

import java.io.IOException;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

public class ShardResponse extends BroadcastShardResponse {
    public  final ShardActionDefinitionBase definition;
    private final TransportItemBase transportItem;

    public ShardResponse(ShardActionDefinitionBase definition, ShardId shardId, TransportItemBase item) {
        super(shardId);
        this.definition = definition;
        this.transportItem = item;
    }

    public ShardResponse(ShardActionDefinitionBase definition, StreamInput in) throws IOException {
        super (in);
        this.definition = definition;
        transportItem = definition.createTransportItem(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        transportItem.writeTo(out);
    }

    public TransportItemBase getTransportItem() {
        return transportItem;
    }

}
