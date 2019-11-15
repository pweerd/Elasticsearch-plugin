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

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

public abstract class ShardActionDefinitionBase extends ActionDefinition {

    public enum ShardsEnum {
        ALL, PRIMARY, ACTIVE, ASSIGNED
    };

    public final ActionType<ShardBroadcastResponse> actionType;
    public final ActionHandler<ShardBroadcastRequest, ShardBroadcastResponse> handler;

    public final ShardsEnum targets;
    public final boolean includeEmptyShards;

    /**
     * Name must start with one of: [cluster:admin, indices:data/read, indices:monitor, indices:data/write, internal:, indices:internal, cluster:monitor, cluster:internal, indices:admin]
     */
    protected ShardActionDefinitionBase(Class<? extends ShardTransportActionBase> transportAction, String name, boolean debug, ShardsEnum targets, boolean includeEmpty) {
        super(name, debug);
        this.targets = targets;
        this.includeEmptyShards = includeEmpty;
        this.actionType = new ActionType<ShardBroadcastResponse>(name, (x)->createBroadcastResponse (x));
        this.handler = new ActionHandler<ShardBroadcastRequest, ShardBroadcastResponse>(actionType, transportAction);
    }

    public abstract TransportItemBase createTransportItem(StreamInput in) throws IOException;
    public abstract TransportItemBase createTransportItem();

    public ShardBroadcastRequest createBroadcastRequest(StreamInput in) throws IOException {
        return new ShardBroadcastRequest(this, in);
    }

    public ShardBroadcastResponse createBroadcastResponse(StreamInput in) throws IOException {
        return new ShardBroadcastResponse(this, in);
    }

    public ShardRequest createShardRequest(StreamInput in) throws IOException {
        return new ShardRequest(this, in);
    }

}
