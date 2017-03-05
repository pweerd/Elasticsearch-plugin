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
    private TransportItemBase transportItem;
    private Throwable error;

    public NodeResponse(TransportItemBase item) {
        transportItem = item;
    }

    public NodeResponse(Throwable err) {
        error = err;
    }

    public NodeResponse(DiscoveryNode node, TransportItemBase item) {
        super(node);
        this.transportItem = item;
    }

    public NodeResponse(DiscoveryNode node, Throwable err) {
        super(node);
        error = err;
    }

    public TransportItemBase getTransportItem() {
        return transportItem;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable err) {
        error = err;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        error = null;
        if (in.readBoolean())
            error = TransportItemBase.readThrowable(in);
        if (in.readBoolean())
            transportItem.readFrom(in);
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

}
