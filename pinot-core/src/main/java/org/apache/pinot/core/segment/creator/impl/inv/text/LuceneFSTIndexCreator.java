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
package org.apache.pinot.core.segment.creator.impl.inv.text;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.core.segment.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.core.util.fst.FSTBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


public class LuceneFSTIndexCreator implements DictionaryBasedInvertedIndexCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
    private final File _fstIndexFile;
    public static final String FST_INDEX_FILE_EXTENSION = ".lucene.fst";
    private final FSTBuilder _fstBuilder;

    public LuceneFSTIndexCreator(
            File indexDir, String columnName,
            int cardinality, String[] sortedEntries) throws IOException {
        _fstIndexFile = new File(
                indexDir, columnName + FST_INDEX_FILE_EXTENSION);

        _fstBuilder = new FSTBuilder();

        for (int dictId = 0; dictId < cardinality; dictId++) {
            if (sortedEntries != null) {
                _fstBuilder.addEntry(sortedEntries[dictId], dictId);
            }
        }
    }

    // add SortedDicts should be called in sorted order.
    public void addSortedDictIds(String document, int dictId) throws IOException {
        _fstBuilder.addEntry(document, dictId);
    }

    @Override
    public void add(int dictId) {
        // NOOP
    }

    @Override
    public void add(int[] dictIds, int length) {
        throw new IllegalStateException(
                "LuceneFSTIndexCreator does not support add interface");
    }

    @Override
    public void seal() throws IOException {
        LOGGER.info("Sealing the segment for column: " + _fstIndexFile.getAbsolutePath());
        FST<Long> fst = _fstBuilder.done();
        FileOutputStream fileOutputStream = new FileOutputStream(_fstIndexFile);
        OutputStreamDataOutput d = new OutputStreamDataOutput(fileOutputStream);
        fst.save(d);
        fileOutputStream.close();
    }

    @Override
    public void close() throws IOException {
    }
}
