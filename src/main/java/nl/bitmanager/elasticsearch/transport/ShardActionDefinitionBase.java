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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

import nl.bitmanager.elasticsearch.support.Utils;

public abstract class ShardActionDefinitionBase
        extends Action<ShardBroadcastRequest, ShardBroadcastResponse, ShardActionDefinitionBase.BroadcastRequestBuilder> {

    public enum ShardsEnum {
        ALL, PRIMARY, ACTIVE, ASSIGNED
    };

    public final boolean debug;
    public final String id;
    public final ShardsEnum targets;
    public final boolean includeEmptyShards;

    protected ShardActionDefinitionBase(String name, boolean debug, ShardsEnum targets, boolean includeEmpty) {
        super(name);
        this.targets = targets;
        this.includeEmptyShards = includeEmpty;
        this.debug = debug;
        this.id = Utils.getTrimmedClass(this);
    }

    public abstract TransportItemBase createTransportItem();

    @Override
    public ShardBroadcastResponse newResponse() {
        return new ShardBroadcastResponse(createTransportItem());
    }

    @Override
    public BroadcastRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        if (debug)
            System.out.printf("[%s]: newRequestBuilder()\n", id);
        return new BroadcastRequestBuilder(client, this);
    }

    public static class BroadcastRequestBuilder
            extends BroadcastOperationRequestBuilder<ShardBroadcastRequest, ShardBroadcastResponse, BroadcastRequestBuilder> {

        public BroadcastRequestBuilder(ElasticsearchClient client, ShardActionDefinitionBase definition) {
            super(client, definition, new ShardBroadcastRequest(definition));
        }
    }

}
