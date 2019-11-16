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

package nl.bitmanager.elasticsearch.extensions.cachedump;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import nl.bitmanager.elasticsearch.extensions.RestControllerWrapper;
import nl.bitmanager.elasticsearch.transport.NodeBroadcastRequest;
import nl.bitmanager.elasticsearch.transport.NodeBroadcastResponse;


public class CacheDumpRestAction extends BaseRestHandler {

   @Inject
   public CacheDumpRestAction(RestControllerWrapper c) {
       c.registerHandler(GET, "/_bm/cache/dump", this);
   }

   @Override
   public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
       final ActionDefinition def = ActionDefinition.INSTANCE;
       final NodeBroadcastRequest broadcastReq = new NodeBroadcastRequest(def, new CacheDumpTransportItem(def, request));

       return channel -> client.execute(
               def.actionType,
               broadcastReq,
               new RestToXContentListener<NodeBroadcastResponse>(channel)
        );
   }

   @Override
   public boolean canTripCircuitBreaker() {
       return false;
   }

    @Override
    public String getName() {
        return ActionDefinition.INSTANCE.name;
    }

}