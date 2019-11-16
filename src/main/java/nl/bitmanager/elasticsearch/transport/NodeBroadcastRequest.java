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

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class NodeBroadcastRequest extends BaseNodesRequest<NodeBroadcastRequest> {
    private final TransportItemBase transportItem;
    public  final NodeActionDefinitionBase definition;
    protected final String id;

    public NodeBroadcastRequest(NodeActionDefinitionBase definition, TransportItemBase transportItem, String... nodesIds) {
        super(nodesIds);
        this.definition = definition;
        this.id = definition.id + ".NodeBroadcastRequest";
        this.transportItem = transportItem;
    }

    public NodeBroadcastRequest(NodeActionDefinitionBase definition, StreamInput in) throws IOException {
        super (in);
        this.definition = definition;
        this.id = definition.id + ".NodeBroadcastRequest";

        this.transportItem = definition.createTransportItem(in);
        if (definition.debug)
            System.out.printf("[%s]: readFrom->%s\n", id, transportItem);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (definition.debug)
            System.out.printf("[%s]: writeTo item=%s\n", id, transportItem);
        super.writeTo(out);
        transportItem.writeTo(out);
    }


    public TransportItemBase getTransportItem() {
        return transportItem;
    }

}
