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

package nl.bitmanager.elasticsearch.extensions.view;

import java.util.HashSet;
import java.util.regex.Pattern;

public class Selector {
   private HashSet<String> selectedSet;
   private Pattern selectedExpr;
   private boolean inversedSet, inversedExpr;
   
   public Selector (String selectedValues, String expr) {
       if (selectedValues != null && selectedValues.length()>0) {
           if (selectedValues.charAt(0) == '!' || selectedValues.charAt(0) == '-') {
               inversedSet = true;
               selectedValues = (selectedValues.length()==1) ? null : selectedValues.substring(1);
           }
           if (selectedValues != null)
               selectedSet = createSet (selectedValues);
       }
       
       if (expr != null && expr.length()>0) {
           if (expr.charAt(0) == '!' || expr.charAt(0) == '-') {
               inversedExpr = true;
               expr = (expr.length()==1) ? null : expr.substring(1);
           }
           if (expr != null)
               selectedExpr = Pattern.compile(expr);
       }
       System.out.println("SEL: " + selectedValues + ", expr=" + expr);
       System.out.println("set=" + selectedSet + ", expr=" + selectedExpr);
   }
   
   private static HashSet<String> createSet (String x) {
       if (x==null || x.length()==0) return null;
       HashSet<String> ret = new HashSet<String>();
       String[] arr = x.split("[,;\\|]");
       for (int i = 0; i < arr.length; i++) {
           String s = arr[i].trim().toLowerCase();
           ret.add(s);
       }
       return ret;
   }
   
   private boolean isSelectedBySet (String v) {
       boolean ret;
       ret = (selectedSet == null) ? true : selectedSet.contains(v);
       return inversedSet ? !ret : ret;
   }
   private boolean isSelectedByExpr (String v) {
       boolean ret;
       ret = (selectedExpr == null) ? true : selectedExpr.matcher(v).find();
       return inversedExpr ? !ret : ret;
   }
   public boolean isSelected (String v) {
       if (v==null) return false;
       
       if (!isSelectedBySet(v)) return false;
       return isSelectedByExpr (v);
   }
}
