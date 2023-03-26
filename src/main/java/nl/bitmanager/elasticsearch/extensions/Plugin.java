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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.search.fetch.FetchSubPhase;

import nl.bitmanager.elasticsearch.extensions.aggregations.UndupByParentsAggregatorBuilder;
import nl.bitmanager.elasticsearch.extensions.queries.AllowNestedQueryBuilder;
import nl.bitmanager.elasticsearch.extensions.queries.FuzzyQueryBuilder;
import nl.bitmanager.elasticsearch.extensions.queries.MatchDeletedQueryBuilder;
import nl.bitmanager.elasticsearch.extensions.queries.MatchNestedQueryBuilder;
import nl.bitmanager.elasticsearch.search.FetchDiagnostics;
import nl.bitmanager.elasticsearch.search.SearchParms;
import nl.bitmanager.elasticsearch.similarity.BoundedSimilarity;
import nl.bitmanager.elasticsearch.support.Utils;

public class Plugin extends org.elasticsearch.plugins.Plugin implements AnalysisPlugin, ActionPlugin, MapperPlugin, SearchPlugin {
    private static final boolean LIMITED = true;
    public static String version;
    public static final String Name = "bitmanager-extensions";
    public static final Logger logger = Loggers.getLogger(Plugin.class, "bitmanager-ext");

    public static Settings ESSettings;

//    public void onModule(SearchModule m) {
//        logger.info("onModule(SearchModule m)");
//        List<org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry> list = m.getNamedXContents();
//        for (org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry x: list) {
//            logger.info("-- " + x.name + ", cat=" + x.categoryClass.getName() + ", tos=" + x);
//
//        }
//
//    }

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

        sb = new StringBuilder();
        sb.append("Settings:\r\n");
        for (String key: settings.keySet()) {
            sb.append("-- ");
            sb.append(key);
            sb.append('=');
            sb.append(settings.get(key));
            sb.append("\r\n");
        }
        logger.info(sb.toString());
    }

    boolean simLogged;
    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (!simLogged) {
            simLogged = true;
            logger.info("Register bounded_similarity");
        }
        indexModule.addSimilarity("bounded_similarity", BoundedSimilarity::create);
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> ret = new HashMap<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>>();
        ret.put("bm_shingle", new nl.bitmanager.elasticsearch.analyses.ShingleFilter.Provider());
        ret.put("bm_word_delimiter_graph", new nl.bitmanager.elasticsearch.analyses.WordDelimiterGraphTokenFilterFactory.Provider());
        logRegistered (ret.keySet(), "token filters");
        return ret;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> ret = Arrays.asList(
                nl.bitmanager.elasticsearch.extensions.version.ActionDefinition.INSTANCE.handler,
                nl.bitmanager.elasticsearch.extensions.view.ActionDefinition.INSTANCE.handler,
                nl.bitmanager.elasticsearch.extensions.termlist.ActionDefinition.INSTANCE.handler,
                nl.bitmanager.elasticsearch.extensions.cachedump.ActionDefinition.INSTANCE.handler);
        logRegistered (ret, "transport actions", (ActionHandler<? extends ActionRequest, ? extends ActionResponse> k)->k.getAction().name());
        return ret;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        ArrayList<RestHandler> ret = new ArrayList<RestHandler>(5);
        RestControllerWrapper c = new RestControllerWrapper(restController);
        ret.add (new nl.bitmanager.elasticsearch.extensions.version.VersionRestAction(c));
        ret.add (new nl.bitmanager.elasticsearch.extensions.help.HelpRestAction(c));
        ret.add (new nl.bitmanager.elasticsearch.extensions.view.ViewRestAction(c));
        ret.add (new nl.bitmanager.elasticsearch.extensions.termlist.TermlistRestAction(c));
        ret.add (new nl.bitmanager.elasticsearch.extensions.cachedump.CacheDumpRestAction(c));
        logger.info(c.toString());
        return ret;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        logRegistered (typeParsers.keySet(), "types");
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
        ret.add (new QuerySpec<>(MatchDeletedQueryBuilder.NAME, MatchDeletedQueryBuilder::new, MatchDeletedQueryBuilder::fromXContent));
        ret.add (new QuerySpec<>(MatchNestedQueryBuilder.NAME, MatchNestedQueryBuilder::new, MatchNestedQueryBuilder::fromXContent));
        ret.add (new QuerySpec<>(AllowNestedQueryBuilder.NAME, AllowNestedQueryBuilder::new, AllowNestedQueryBuilder::fromXContent));
        ret.add (new QuerySpec<>(FuzzyQueryBuilder.NAME, FuzzyQueryBuilder::new, FuzzyQueryBuilder::fromXContent));
        logRegistered (ret, "queries", (QuerySpec<?> qs)->qs.getName().getPreferredName());
        return ret;
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        List<FetchSubPhase> ret = new ArrayList<FetchSubPhase>(1);
        ret.add (new FetchDiagnostics ());
        logRegistered (ret, "fetch-phases", (FetchSubPhase k)->k.getClass().getSimpleName());
        return ret;
    }

    @Override
    public List<SearchExtSpec<?>> getSearchExts() {
        List<SearchExtSpec<?>> ret = new ArrayList<SearchExtSpec<?>>(1);
        ret.add(SearchParms.createSpec());
        logRegistered (ret, "search-exts", (SearchExtSpec<?> k)->k.getName().getPreferredName());
        return ret;
    }



    @Override
    public List<AggregationSpec> getAggregations() {
        List<AggregationSpec> ret = new ArrayList<AggregationSpec>(1);
        ret.add (UndupByParentsAggregatorBuilder.createAggregationSpec());
        logRegistered (ret, "aggregations", (AggregationSpec k)->k.getName().getPreferredName());
        return ret;
    }

    private static <T1> void logRegistered (Collection<T1> list, String what) {
        logRegistered (list, what, (T1 t)->t.toString());
    }
    private static <T1> void logRegistered (Collection<T1> list, String what, Function<T1, String> dlg) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        sb.append("Register ");
        sb.append(list.size());
        sb.append(' ');
        sb.append(what);
        sb.append(": ");

        for (T1 t: list) {
            if (first) first = false; else sb.append("; ");
            sb.append(dlg.apply(t));
        }
        logger.info(sb.toString());
    }

    private static final Map<String, Mapper.TypeParser> typeParsers;
    static {
        Map<String, Mapper.TypeParser> tmp = new HashMap<String, Mapper.TypeParser>(2);
        //pw7 tmp.put (TextFieldWithDocvaluesMapper.CONTENT_TYPE, new TextFieldWithDocvaluesMapper.TypeParser());
        //pw7 tmp.put ("analyzed_keyword", new TextFieldWithDocvaluesMapper.TypeParser());
        typeParsers = tmp;
    }
}
