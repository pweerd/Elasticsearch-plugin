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

public abstract class NodeActionDefinitionBase extends ActionDefinition {
    public final ActionType<NodeBroadcastResponse> actionType;
    public final ActionHandler<NodeBroadcastRequest, NodeBroadcastResponse> handler;
 
    /**
     * Name must start with one of: [cluster:admin, indices:data/read, indices:monitor, indices:data/write, internal:, indices:internal, cluster:monitor, cluster:internal, indices:admin]
     */
    protected NodeActionDefinitionBase(Class<? extends NodeTransportActionBase> transportAction, String name, boolean debug) {
        super(name, debug);
        this.actionType = new ActionType<NodeBroadcastResponse>(name, (x)->createBroadcastResponse (x));
        this.handler = new ActionHandler<NodeBroadcastRequest, NodeBroadcastResponse>(actionType, transportAction);
    }

    public abstract TransportItemBase createTransportItem();
    public abstract TransportItemBase createTransportItem(StreamInput in) throws IOException;

    public NodeBroadcastRequest createBroadcastRequest(StreamInput in) throws IOException {
        return new NodeBroadcastRequest(this, in);
    }
    public NodeBroadcastResponse createBroadcastResponse(StreamInput in) throws IOException {
        return new NodeBroadcastResponse(this, in);
    }

    public NodeRequest createNodeRequest(StreamInput in) throws IOException {
        return new NodeRequest(this, in);
    }

}
