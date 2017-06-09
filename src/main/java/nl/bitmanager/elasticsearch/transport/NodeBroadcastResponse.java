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
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class NodeBroadcastResponse extends BaseNodesResponse<NodeResponse> implements ToXContentObject {
    public final NodeActionDefinitionBase definition;
    private TransportItemBase transportItem;

    public NodeBroadcastResponse(NodeActionDefinitionBase action) {
        definition = action;
        transportItem = action.createTransportItem();
    }

    public NodeBroadcastResponse(NodeActionDefinitionBase action, ClusterName clusterName, List<NodeResponse> responses,  List<FailedNodeException> failures) {
        super(clusterName, responses, failures);
        definition = action;
        TransportItemBase result = null;
        if (responses != null) {
            for (NodeResponse resp : responses) {
                TransportItemBase item = resp.getTransportItem();
                if (result == null)
                    result = item;
                else
                    result.consolidateResponse(item);
            }
        }
        transportItem = result != null ? result : action.createTransportItem();
    }

    public TransportItemBase getTransportItem() {
        return transportItem;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        transportItem.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        transportItem.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        // PW buildBroadcastNodesHeader (builder);
        builder.startObject();
        if (transportItem != null) 
            transportItem.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    // PW
    // public void buildBroadcastNodesHeader(XContentBuilder builder) throws
    // IOException {
    // builder.startObject("_nodes");
    // int failed=0;
    // int success=0;
    //
    // for (NodeResponse x: super.nodes) {
    // if (x.getError()==null) success++; else failed++;
    // }
    // builder.field("total", failed+success);
    // builder.field("successful", success);
    // builder.field("failed", failed);
    // if (failed != 0) {
    // builder.startArray("failures");
    // for (NodeResponse x: super.nodes) {
    // if (x.getError()==null) continue;
    // builder.startObject();
    // builder.field("node", x.getNode().toString());
    // builder.field("reason", x.getError().toString());
    // builder.endObject();
    // }
    // builder.endArray();
    // }
    // builder.endObject();
    // }

    public NodeResponse readResponse(StreamInput in) throws IOException {
        NodeResponse resp = new NodeResponse(definition.createTransportItem());
        resp.readFrom(in);
        return resp;
    }

    @Override
    protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList((s) -> readResponse(s));
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

}
