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

//PW moet weg

//package nl.bitmanager.elasticsearch.analyses;
//
//import org.elasticsearch.indices.analysis.AnalysisModule;
//
//public class AnalysisBinderProcessor extends AnalysisModule.AnalysisBinderProcessor {
//
//   @Override
//   public void processTokenizers(TokenizersBindings tokenizersBindings) {
//   }
//
//   @Override
//   public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {
//       tokenFiltersBindings.processTokenFilter("bm_global_unique", nl.bitmanager.elasticsearch.analyses.unique.GlobalUniqueTokenFilter.Factory.class);
//       tokenFiltersBindings.processTokenFilter("bm_skip", nl.bitmanager.elasticsearch.analyses.unique.SkipTokenFilter.Factory.class);
//       tokenFiltersBindings.processTokenFilter("bm_fieldcount", nl.bitmanager.elasticsearch.analyses.fieldcountfilter.FieldCountFilter.Factory.class);
//   }
//
//   @Override
//   public void processCharFilters(CharFiltersBindings charFiltersBindings) {
//   }
//
//}
