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
package org.apache.pinot.core.segment.index.readers;

import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.core.util.fst.RegexpMatcher;
import org.apache.pinot.core.util.fst.PinotBufferIndexInput;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;

public class LuceneFSTIndexReader implements TextIndexReader {
    public static final Logger LOGGER =
            LoggerFactory.getLogger(LuceneFSTIndexReader.class);

    private final PinotDataBuffer _dataBuffer;
    private final PinotBufferIndexInput _dataBufferIndexInput;
    private final FST<Long> readFST;
    private String dir;

    public LuceneFSTIndexReader(
            File segmentIndexDir, String column) throws IOException {
        File segmentsV3Dir = SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir);

        File fstFile = new File(segmentsV3Dir,
                column + LuceneFSTIndexCreator.FST_INDEX_FILE_EXTENSION);
        dir = segmentsV3Dir.getAbsolutePath();

        this._dataBuffer = PinotDataBuffer.mapFile(
                fstFile, true,0,
                fstFile.length(),
                ByteOrder.BIG_ENDIAN, "fstIndexFile");
        this._dataBufferIndexInput = new PinotBufferIndexInput(
                this._dataBuffer, 0L, this._dataBuffer.size());

        this.readFST = new FST(
                this._dataBufferIndexInput,
                PositiveIntOutputs.getSingleton(),
                new OffHeapFSTStore());
    }

    @Override
    public MutableRoaringBitmap getDocIds(String searchQuery) {
        throw new RuntimeException(
                "LuceneFSTIndexReader only supports getDictIds currently.");
    }


    @Override
    public ImmutableRoaringBitmap getDictIds(String searchQuery) {
        try {
            MutableRoaringBitmap dictIds = new MutableRoaringBitmap();
            List<Long> matchingIds = RegexpMatcher.regexMatch(searchQuery, this.readFST);
            for (Long matchingId : matchingIds) {
                dictIds.add(matchingId.intValue());
            }
            return dictIds.toImmutableRoaringBitmap();
        } catch (Exception ex) {
            LOGGER.error("Error getting matching Ids from FST", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        // Do Nothing
    }
}
