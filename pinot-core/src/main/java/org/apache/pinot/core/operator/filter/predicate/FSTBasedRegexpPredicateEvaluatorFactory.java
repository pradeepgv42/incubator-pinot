/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.filter.predicate;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;
import org.apache.pinot.core.util.fst.RegexpMatcher;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.apache.pinot.core.query.request.context.predicate.Predicate.Type.REGEXP_LIKE;

public class FSTBasedRegexpPredicateEvaluatorFactory {
    public FSTBasedRegexpPredicateEvaluatorFactory() {}

    public static BaseDictionaryBasedPredicateEvaluator newFSTBasedEvaluator(
            TextIndexReader fstIndexReader, Dictionary dictionary, String regexpQuery) {
        return new FSTBasedRegexpPredicateEvaluatorFactory.FSTBasedRegexpPredicateEvaluator(
                fstIndexReader, dictionary, regexpQuery);
    }

    public static BaseDictionaryBasedPredicateEvaluator newAutomatonBasedEvaluator(
            Dictionary dictionary, String regexpQuery) {
        return new FSTBasedRegexpPredicateEvaluatorFactory.AutomatonBasedRegexpPredicateEvaluator(
                regexpQuery, dictionary);
    }

    private static class AutomatonBasedRegexpPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
        private final RegexpMatcher _regexpMatcher;
        private final Dictionary _dictionary;
        int[] _matchingDictIds;

        public AutomatonBasedRegexpPredicateEvaluator(String searchQuery, Dictionary dictionary) {
            _regexpMatcher = new RegexpMatcher(searchQuery, null);
            _dictionary = dictionary;
        }

        @Override
        public Predicate.Type getPredicateType() {
            return REGEXP_LIKE;
        }

        @Override
        public boolean applySV(int dictId) {
            return _regexpMatcher.match(_dictionary.getStringValue(dictId));
        }

        @Override
        public int[] getMatchingDictIds() {
            if (_matchingDictIds == null) {
                IntList matchingDictIds = new IntArrayList();
                int dictionarySize = _dictionary.length();
                for (int dictId = 0; dictId < dictionarySize; dictId++) {
                    if (applySV(dictId)) {
                        matchingDictIds.add(dictId);
                    }
                }
                _matchingDictIds = matchingDictIds.toIntArray();
            }
            return _matchingDictIds;
        }
    }

    private static class FSTBasedRegexpPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
        private final TextIndexReader _fstIndexReader;
        private final String _searchQuery;
        private final ImmutableRoaringBitmap _dictIds;
        private final Dictionary _dictionary;

        public FSTBasedRegexpPredicateEvaluator(
                TextIndexReader fstIndexReader,
                Dictionary dictionary,
                String searchQuery) {
            _dictionary = dictionary;
            _fstIndexReader = fstIndexReader;
            _searchQuery = searchQuery;
            _dictIds = _fstIndexReader.getDictIds(_searchQuery);
        }

        @Override
        public boolean isAlwaysFalse() {
            return _dictIds.isEmpty();
        }

        @Override
        public boolean isAlwaysTrue() {
            return _dictIds.getCardinality() == _dictionary.length();
        }

        @Override
        public Predicate.Type getPredicateType() {
            return REGEXP_LIKE;
        }

        @Override
        public boolean applySV(int dictId) {
            return _dictIds.contains(dictId);
        }

        @Override
        public int[] getMatchingDictIds() {
            return _dictIds.toArray();
        }
    }
}
