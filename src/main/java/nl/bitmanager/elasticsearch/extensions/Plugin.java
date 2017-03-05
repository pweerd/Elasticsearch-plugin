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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestHandler;
//import java.util.function.BiFunction;

import nl.bitmanager.elasticsearch.analyses.TokenFilterProvider;
import nl.bitmanager.elasticsearch.mappers.TextFieldWithDocvaluesMapper;
import nl.bitmanager.elasticsearch.similarity.BoundedSimilarity;
import nl.bitmanager.elasticsearch.support.Utils;

public class Plugin extends org.elasticsearch.plugins.Plugin implements AnalysisPlugin, ActionPlugin, MapperPlugin {// implements
                                                                                                      // AnalysisPlugin,
                                                                                                      // ActionPlugin,
                                                                                                      // {
    public static String version;
    public static final String Name = "bitmanager-extensions";
    public static final Logger logger = Loggers.getLogger("bitmanager-ext");

    public static Settings ESSettings;

    @Inject
    public Plugin(Settings settings) {
        ESSettings = settings;
        Map<String, String> map;
        StringBuilder sb;

        try {
            map = Utils.getManifestEntries();
            version = map.get("build-version");
            sb = new StringBuilder();
            sb.append(getClass().getSimpleName());
            sb.append(" version ");
            sb.append(version);
            sb.append(" loading. Build at ");
            sb.append(map.get("build-date"));
            sb.append(", SHA=");
            sb.append(map.get("git-commit"));
            sb.append("...");
            logger.info(sb.toString());
        } catch (Throwable e) {
            logger.error("Error during load " + Name + ": " + e.getMessage(), e);
        }
        map = settings.getAsMap();
        sb = new StringBuilder();
        sb.append("Settings:\r\n");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append("-- ");
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(entry.getValue());
            sb.append("\r\n");
        }
        logger.info(sb.toString());
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addSimilarity("bounded_similarity",
                (name, settings) -> new BoundedSimilarity.Provider(name, settings));
    }

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return TokenFilterProvider.allFilters;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        logger.info("returning 4 transport actions");
        return Arrays.asList(nl.bitmanager.elasticsearch.extensions.version.ActionDefinition.HANDLER,
                nl.bitmanager.elasticsearch.extensions.view.ActionDefinition.HANDLER,
                nl.bitmanager.elasticsearch.extensions.termlist.ActionDefinition.HANDLER,
                nl.bitmanager.elasticsearch.extensions.cachedump.ActionDefinition.HANDLER);
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        logger.info("returning 4 rest actions");
        return Arrays.asList(nl.bitmanager.elasticsearch.extensions.version.VersionRestAction.class,
                nl.bitmanager.elasticsearch.extensions.help.HelpRestAction.class,
                nl.bitmanager.elasticsearch.extensions.view.ViewRestAction.class,
                nl.bitmanager.elasticsearch.extensions.termlist.TermlistRestAction.class,
                nl.bitmanager.elasticsearch.extensions.cachedump.CacheDumpRestAction.class);
    }
    
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return typeParsers;
    }

    /**
     * Returns additional metadata mapper implementations added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the metadata mapper, which
     * is used in the mapping json to configure the metadata mapper, and the value is a
     * {@link MetadataFieldMapper.TypeParser} to parse the mapper settings into a
     * {@link MetadataFieldMapper}.
     */
    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.emptyMap();
    }

    private static final Map<String, Mapper.TypeParser> typeParsers;
    static {
        Map<String, Mapper.TypeParser> tmp = new HashMap<String, Mapper.TypeParser>(1);
        tmp.put (TextFieldWithDocvaluesMapper.CONTENT_TYPE, new TextFieldWithDocvaluesMapper.TypeParser());
        tmp.put ("analyzed_keyword", new TextFieldWithDocvaluesMapper.TypeParser());
        typeParsers = tmp;
    }
}
