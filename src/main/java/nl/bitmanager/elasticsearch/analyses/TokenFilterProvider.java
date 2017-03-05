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

package nl.bitmanager.elasticsearch.analyses;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;

public class TokenFilterProvider implements AnalysisProvider<TokenFilterFactory> {
    public final String filterName;
    private final FactoryID factoryId;

    public enum FactoryID {
        BM_GLOBAL_UNIQUE, BM_SKIP, BM_FIELDCOUNT
    };

    public TokenFilterProvider(String filterName, FactoryID factoryId) {
        this.filterName = filterName;
        this.factoryId = factoryId;
    }

    @Override
    public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings)
            throws IOException {
        switch (factoryId) {
        case BM_GLOBAL_UNIQUE:
            return new GlobalUniqueTokenFilter.Factory(indexSettings, name, settings);
        case BM_SKIP:
            return new SkipTokenFilter.Factory(indexSettings, name, settings);
        case BM_FIELDCOUNT:
            return new FieldCountFilter.Factory(indexSettings, name, settings);
        }
        throw new RuntimeException("Unrecognized FactoryID: " + factoryId);
    }

    public static final Map<String, AnalysisProvider<TokenFilterFactory>> allFilters;
    static {
        HashMap<String, AnalysisProvider<TokenFilterFactory>> map = new HashMap<String, AnalysisProvider<TokenFilterFactory>>();
        addTokenFilterProvider(map, "bm_global_unique", FactoryID.BM_GLOBAL_UNIQUE);
        addTokenFilterProvider(map, "bm_skip", FactoryID.BM_SKIP);
        addTokenFilterProvider(map, "bm_fieldcount", FactoryID.BM_FIELDCOUNT);
        allFilters = map;
    }

    static void addTokenFilterProvider(HashMap<String, AnalysisProvider<TokenFilterFactory>> map, String name,
            FactoryID id) {
        TokenFilterProvider provider = new TokenFilterProvider(name, id);
        map.put(provider.filterName, provider);
    }

}
