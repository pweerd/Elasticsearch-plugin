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

package nl.bitmanager.elasticsearch.extensions.termlist;

public class SortType {
    public final static int SORT_TERM = 1;
    public final static int SORT_COUNT = 2;
    public final static int SORT_REVERSE = 0x100;
    public final int order;    

    public SortType (String sort) {
        int sortType = SORT_TERM;
        if (sort != null && sort.length() > 0) {
            sort = sort.toLowerCase();
            if (sort.startsWith("term")) 
                sortType = SORT_TERM;
            else if (sort.startsWith("-term")) 
                sortType = SORT_TERM | SORT_REVERSE;
            else if (sort.startsWith("count"))
                sortType = SORT_COUNT;
            else if (sort.startsWith("-count"))
                sortType = SORT_COUNT | SORT_REVERSE;
            else
                throw new RuntimeException("Wrong sort. Possible values are <term|-term|count|-count>");
        }
        this.order = sortType;
    }

    public SortType() {
        this.order = SORT_TERM;
    }
    public SortType(int v) {
        this.order = v;
    }
    
    public String toString() {
        switch (order) {
        case SORT_TERM: return "term";
        case SORT_TERM | SORT_REVERSE: return "-term";
        case SORT_COUNT: return "count";
        case SORT_COUNT | SORT_REVERSE: return "-count";
        default: return String.format("Unknown sorttype [%08X]", order);
        }
    }


}
