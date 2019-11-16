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

package nl.bitmanager.elasticsearch.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;

public class RestControllerWrapper  {
    private final RestController _controller;
    private final ArrayList<String> items;

    public RestControllerWrapper (RestController wrapped) {
        this._controller = wrapped;
        items = new ArrayList<String> (8);
    }

    public void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        _controller.registerHandler(method, path, handler);
        items.add (String.format("%s (%s)", path, method.toString().toLowerCase()));
    }

    public List<String> items() {return items;}

    @Override
    public String toString() {
        Collections.sort(items);

        StringBuilder sb = new StringBuilder();
        sb.append("Registered ");
        sb.append(items.size());
        sb.append(" rest actions:");
        boolean first = true;
        for (String x: items) {
            if (first) first = false; else sb.append("; ");
            sb.append(x);
        }
        return sb.toString();
    }

}
