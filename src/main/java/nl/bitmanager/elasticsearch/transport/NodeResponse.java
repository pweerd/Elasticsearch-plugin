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

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class NodeResponse extends BaseNodeResponse {
    private final NodeActionDefinitionBase definition;
    private final TransportItemBase transportItem;
    private final Throwable error;

    public NodeResponse (NodeActionDefinitionBase definition,  DiscoveryNode node, TransportItemBase transportItem)
    {
        super (node);
        this.definition = definition;
        this.transportItem = transportItem;
        this.error = null;
    }
    public NodeResponse (NodeActionDefinitionBase definition,  DiscoveryNode node, Throwable error)
    {
        super (node);
        this.definition = definition;
        this.transportItem = definition.createTransportItem();
        this.error = error;
    }
    public NodeResponse (NodeActionDefinitionBase definition,  StreamInput in) throws IOException
    {
        super(in);
        this.definition = definition;
        error = in.readBoolean() ? TransportItemBase.readThrowable(in) : null;
        transportItem = in.readBoolean() ? definition.createTransportItem(in) : definition.createTransportItem();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(error != null);
        if (error != null)
            TransportItemBase.writeThrowable(out, error);
        out.writeBoolean(transportItem != null);
        if (transportItem != null)
            transportItem.writeTo(out);
    }

    public TransportItemBase getTransportItem() {
        return transportItem;
    }

    public Throwable getError() {
        return error;
    }

}
