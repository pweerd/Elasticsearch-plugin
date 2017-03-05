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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import nl.bitmanager.elasticsearch.transport.NodeActionDefinitionBase;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

public class ActionDefinition extends NodeActionDefinitionBase {

	public static final ActionDefinition INSTANCE;
    public static final ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse> HANDLER;
    static {
		INSTANCE = new ActionDefinition();
        HANDLER = new ActionHandler(INSTANCE, TransportAction.class);
    }

   private ActionDefinition() {
      super("cache/dump", true);
   }

    @Override
    public TransportItemBase createTransportItem() {
        return new CacheDumpTransportItem();
    }

}
