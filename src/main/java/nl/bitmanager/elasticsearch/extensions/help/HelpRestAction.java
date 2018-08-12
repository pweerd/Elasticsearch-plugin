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

package nl.bitmanager.elasticsearch.extensions.help;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import nl.bitmanager.elasticsearch.extensions.RestControllerWrapper;
import nl.bitmanager.elasticsearch.extensions.view.ActionDefinition;
import nl.bitmanager.elasticsearch.support.HtmlRestResponse;
import nl.bitmanager.elasticsearch.support.Utils;

public class HelpRestAction extends BaseRestHandler {

    @Inject
    public HelpRestAction(Settings settings, RestControllerWrapper c) {
        super(settings);
        c.registerHandler(GET, "/_bm", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        HtmlRestResponse resp;
        String accept = request.header("Accept");
        System.out.println("HELP: Accept=" + accept);

        if (accept != null && (accept.indexOf("application/json") >=0 || accept.indexOf("text/javascript") >= 0 )) {
            System.out.println("HELP: sending json");
            InputStream strm = this.getClass().getClassLoader().getResourceAsStream("help.json");
            System.out.println("strm=" + strm + ", client=" + Utils.getTrimmedClass(client));
            int avail = strm.available();
            System.out.println("avail=" + avail);
            byte[] respBytes = new byte[avail];
            strm.read(respBytes);
            strm.close();
            resp = new HtmlRestResponse(RestStatus.OK, respBytes);
        } else {
            System.out.println("HELP: sending redirect");
            resp = new HtmlRestResponse(RestStatus.FOUND, "https://github.com/pweerd/Elasticsearch-plugin/#bitmanagers-elasticsearch-plugin".getBytes("UTF-8"));
            resp.addHeader("Location", "https://github.com/pweerd/Elasticsearch-plugin/#bitmanagers-elasticsearch-plugin");
        }
        return channel -> channel.sendResponse(resp);
    }

    @Override
    public String getName() {
        return ActionDefinition.INSTANCE.name();
    }
}