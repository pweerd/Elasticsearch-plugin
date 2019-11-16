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

import static org.elasticsearch.action.support.DefaultShardOperationFailedException.readShardOperationFailed;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.action.RestActions;

public class ShardBroadcastResponse extends BroadcastResponse implements ToXContentObject {
    protected final ShardActionDefinitionBase definition;
    private   final TransportItemBase transportItem;

    public ShardBroadcastResponse(ShardActionDefinitionBase definition, TransportItemBase item) {
        this.definition = definition;
        transportItem = item;
    }

    public ShardBroadcastResponse(ShardActionDefinitionBase definition, TransportItemBase item, int totalShards, int successfulShards, int failedShards,
            List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.definition = definition;
        this.transportItem = item;
    }

    public ShardBroadcastResponse(ShardActionDefinitionBase definition, StreamInput in) throws IOException {
        super(in);
        this.definition = definition;
        this.transportItem = definition.createTransportItem(in);
    }


    public TransportItemBase getTransportItem() {
        return transportItem;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        transportItem.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, params, this);
        if (transportItem != null)
            transportItem.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
