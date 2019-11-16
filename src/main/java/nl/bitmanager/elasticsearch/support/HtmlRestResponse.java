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

package nl.bitmanager.elasticsearch.support;

import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;

public class HtmlRestResponse extends BytesRestResponse {
   public final static String CONTENT_TYPE_HTML = "text/html; charset=UTF-8";

   public HtmlRestResponse(RestStatus status, String content) {
       super(status, CONTENT_TYPE_HTML, content);
    }
   public HtmlRestResponse(RestStatus status, byte[] content) {
       super(status, CONTENT_TYPE_HTML, content);
    }
}
