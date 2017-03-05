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
import java.util.function.Supplier;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

public class ShardRequest extends BroadcastShardRequest {
    public final ShardActionDefinitionBase definition;
    protected final String id;
    private TransportItemBase transportItem;

    public ShardRequest(ShardActionDefinitionBase definition) {
        this.definition = definition;
        this.id = getId(definition);
    }

    public ShardRequest(ShardId shardId, ShardBroadcastRequest request) {
        super(shardId, request);
        this.definition = request.definition;
        this.id = getId(definition);
        this.transportItem = request.getTransportItem();
    }

    private static String getId(ShardActionDefinitionBase definition) {
        return definition.id + ".ShardRequest";
    }

    public TransportItemBase getTransportItem() {
        return transportItem;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (definition.debug)
            System.out.printf("[%s]: readFrom\n", id);
        super.readFrom(in);
        transportItem = definition.createTransportItem();
        transportItem.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (definition.debug)
            System.out.printf("[%s]: writeTo item=%s\n", id, transportItem);
        super.writeTo(out);
        transportItem.writeTo(out);
    }

    /** Factory to passthrough the actiondefinition */
    public static class Factory implements Supplier<ShardRequest> {
        public final ShardActionDefinitionBase definition;

        public Factory(ShardActionDefinitionBase definition) {
            this.definition = definition;
        }

        @Override
        public ShardRequest get() {
            if (definition.debug)
                System.out.printf("%s: new ShardRequest()\n", definition.id);
            return new ShardRequest(definition);
        }

    }
}
