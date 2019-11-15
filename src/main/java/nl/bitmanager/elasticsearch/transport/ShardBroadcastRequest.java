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

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ShardBroadcastRequest extends BroadcastRequest<ShardBroadcastRequest> {
    public final ShardActionDefinitionBase definition;
    private final TransportItemBase transportItem;
    protected final String id;

    public ShardBroadcastRequest(ShardActionDefinitionBase definition, TransportItemBase transportItem, String indexes) {
        super(Strings.splitStringByCommaToArray(indexes));
        this.definition = definition;
        this.id = definition.id + ".ShardBroadcastRequest";
        this.transportItem = transportItem;
    }

    public ShardBroadcastRequest(ShardActionDefinitionBase definition, StreamInput in) throws IOException {
        super(in);
        this.definition = definition;
        this.id = definition.id + ".ShardBroadcastRequest";
        if (definition.debug)
            System.out.printf("[%s]: readFrom\n", id);
        transportItem = definition.createTransportItem();
    }


    public ShardBroadcastRequest(ShardActionDefinitionBase definition) {
        this(definition, definition.createTransportItem(), null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (definition.debug)
            System.out.printf("[%s]: writeTo item=%s\n", id, transportItem);
        super.writeTo(out);
        transportItem.writeTo(out);
    }


    // public ShardBroadcastRequest(TransportItemBase item) {
    // transportItem = item;
    // }

    // public static ShardBroadcastRequest create(RestRequest req, boolean
    // mandatory) {
    // return create(null, req.param("index"), mandatory);
    // }
    //
    // public static ShardBroadcastRequest create(TransportItemBase item,
    // RestRequest req, boolean mandatory) {
    // return create(item, req.param("index"), mandatory);
    // }
    //
    // public static ShardBroadcastRequest create(TransportItemBase item, String
    // indices, boolean mandatory) {
    // String[] arr = null;
    // if (indices == null || indices.length() == 0) {
    // if (mandatory)
    // throw new RuntimeException("Missing index param. A list of indexes before
    // this command is required.");
    // } else {
    // arr = Strings.splitStringByCommaToArray(indices);
    // }
    // return new ShardBroadcastRequest(item, arr);
    // }

    // public ShardBroadcastRequest(TransportItemBase item, String... indices) {
    // super(indices);
    //
    // // operationThreading(BroadcastOperationThreading.THREAD_PER_SHARD);
    // transportItem = item;
    // }

    public TransportItemBase getTransportItem() {
        return transportItem;
    }


}
