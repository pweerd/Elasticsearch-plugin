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

package nl.bitmanager.elasticsearch.extensions.view;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import nl.bitmanager.elasticsearch.extensions.RestControllerWrapper;
import nl.bitmanager.elasticsearch.transport.ShardBroadcastRequest;
import nl.bitmanager.elasticsearch.transport.ShardBroadcastResponse;


public class ViewRestAction extends BaseRestHandler {

    @Inject
    public ViewRestAction(Settings settings, RestControllerWrapper c) {
        super(settings);
        c.registerHandler(GET, "/{index}/{type}/{id}/_view", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ViewTransportItem item = new ViewTransportItem(request);
        ShardBroadcastRequest broadcastRequest = new ShardBroadcastRequest(ActionDefinition.INSTANCE, item, request.param("index"));
        try {
            return channel -> client.admin().indices().execute(ActionDefinition.INSTANCE, broadcastRequest,
                    new RestToXContentListener<ShardBroadcastResponse>(channel));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public String getName() {
        return ActionDefinition.INSTANCE.name();
    }

}