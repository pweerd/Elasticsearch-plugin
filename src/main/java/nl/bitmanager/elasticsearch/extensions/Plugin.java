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

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.AggregationSpec;
import org.elasticsearch.plugins.SearchPlugin.FetchPhaseConstructionContext;
import org.elasticsearch.plugins.SearchPlugin.SearchExtSpec;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.children.InternalChildren;
import org.elasticsearch.search.fetch.FetchSubPhase;

import nl.bitmanager.elasticsearch.analyses.TokenFilterProvider;
import nl.bitmanager.elasticsearch.extensions.aggregations.InternalParentsAggregation;
import nl.bitmanager.elasticsearch.extensions.aggregations.ParentsAggregatorBuilder;
import nl.bitmanager.elasticsearch.extensions.queries.MatchDeletedQuery;
import nl.bitmanager.elasticsearch.extensions.queries.MatchDeletedQueryBuilder;
import nl.bitmanager.elasticsearch.mappers.TextFieldWithDocvaluesMapper;
import nl.bitmanager.elasticsearch.search.FetchDiagnostics;
import nl.bitmanager.elasticsearch.search.SearchParms;
import nl.bitmanager.elasticsearch.similarity.BoundedSimilarity;
import nl.bitmanager.elasticsearch.support.Utils;

public class Plugin extends org.elasticsearch.plugins.Plugin implements AnalysisPlugin, ActionPlugin, MapperPlugin, SearchPlugin {

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
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        logger.info("returning 4 transport actions");
        return Arrays.asList(nl.bitmanager.elasticsearch.extensions.version.ActionDefinition.HANDLER,
                nl.bitmanager.elasticsearch.extensions.view.ActionDefinition.HANDLER,
                nl.bitmanager.elasticsearch.extensions.termlist.ActionDefinition.HANDLER,
                nl.bitmanager.elasticsearch.extensions.cachedump.ActionDefinition.HANDLER);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        ArrayList<RestHandler> ret = new ArrayList<RestHandler>(4);
        ret.add (new nl.bitmanager.elasticsearch.extensions.version.VersionRestAction(settings, restController));
        ret.add (new nl.bitmanager.elasticsearch.extensions.help.HelpRestAction(settings, restController));
        ret.add (new nl.bitmanager.elasticsearch.extensions.view.ViewRestAction(settings, restController));
        ret.add (new nl.bitmanager.elasticsearch.extensions.termlist.TermlistRestAction(settings, restController));
        ret.add (new nl.bitmanager.elasticsearch.extensions.cachedump.CacheDumpRestAction(settings, restController));
        return ret;
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
    
    @Override
    public List<QuerySpec<?>> getQueries() {
        List<QuerySpec<?>> ret = new ArrayList<QuerySpec<?>>(1);
        QuerySpec<?> x = new QuerySpec<>(MatchDeletedQuery.NAME, MatchDeletedQueryBuilder::new, MatchDeletedQueryBuilder::fromXContent);
        ret.add (x);

        return ret;
    }
    
    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        List<FetchSubPhase> ret = new ArrayList<FetchSubPhase>(1);
        FetchSubPhase x = new FetchDiagnostics ();
        ret.add (x);
        return ret;
    }
    
    @Override
    public List<SearchExtSpec<?>> getSearchExts() {
        List<SearchExtSpec<?>> ret = new ArrayList<SearchExtSpec<?>>(1);
        ret.add(SearchParms.createSpec());
        return ret;
    }


    
    @Override
    public List<AggregationSpec> getAggregations() {
        List<AggregationSpec> ret = new ArrayList<AggregationSpec>(1);
        ret.add (ParentsAggregatorBuilder.createAggregationSpec());
        return ret;
    }



    private static final Map<String, Mapper.TypeParser> typeParsers;
    static {
        Map<String, Mapper.TypeParser> tmp = new HashMap<String, Mapper.TypeParser>(2);
        tmp.put (TextFieldWithDocvaluesMapper.CONTENT_TYPE, new TextFieldWithDocvaluesMapper.TypeParser());
        tmp.put ("analyzed_keyword", new TextFieldWithDocvaluesMapper.TypeParser());
        typeParsers = tmp;
    }
}
