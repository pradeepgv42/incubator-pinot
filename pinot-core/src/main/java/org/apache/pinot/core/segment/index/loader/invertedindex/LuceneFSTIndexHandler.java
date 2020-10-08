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

package org.apache.pinot.core.segment.index.loader.invertedindex;

import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.core.segment.index.readers.StringDictionary;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LuceneFSTIndexHandler {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LuceneFSTIndexHandler.class);

    private final File _indexDir;
    private final SegmentDirectory.Writer _segmentWriter;
    private final String _segmentName;
    private final SegmentVersion _segmentVersion;
    private final Set<ColumnMetadata> _fstIndexColumns = new HashSet<>();


    public LuceneFSTIndexHandler(
            File indexDir, SegmentMetadataImpl segmentMetadata, Set<String> fstIndexColumns,
            SegmentDirectory.Writer segmentWriter) {
        _indexDir = indexDir;
        _segmentWriter = segmentWriter;
        _segmentName = segmentMetadata.getName();
        _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

        for (String column : fstIndexColumns) {
            ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
            if (columnMetadata != null) {
                _fstIndexColumns.add(columnMetadata);
            }
        }
    }

    private void checkUnsupportedOperationsForFSTIndex(ColumnMetadata columnMetadata) {
        String column = columnMetadata.getColumnName();
        if (columnMetadata.getDataType() != FieldSpec.DataType.STRING) {
            throw new UnsupportedOperationException("FST index is currently only supported on STRING columns: " + column);
        }

        if (!columnMetadata.hasDictionary()) {
            throw new UnsupportedOperationException("FST index is currently only supported on dictionary encoded columns: " + column);
        }

        if (!columnMetadata.isSingleValue()) {
            throw new UnsupportedOperationException(
                    "Text index is currently not supported on multi-value columns: " + column);
        }
    }

    public void createFSTIndexesOnSegmentLoad()
            throws Exception {
        for (ColumnMetadata columnMetadata : _fstIndexColumns) {
            checkUnsupportedOperationsForFSTIndex(columnMetadata);
            createFSTIndexForColumn(columnMetadata);
        }
    }

    private BaseImmutableDictionary getDictionaryReader(ColumnMetadata columnMetadata)
            throws IOException {
        PinotDataBuffer dictionaryBuffer =
                _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
        return new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(), columnMetadata.getColumnMaxLength(),
                (byte) columnMetadata.getPaddingCharacter());
    }

    private void createFSTIndexForColumn(ColumnMetadata columnMetadata) throws IOException {
        String column = columnMetadata.getColumnName();
        boolean hasDictionary = columnMetadata.hasDictionary();

        if (!hasDictionary) {
            return;
        }

        if (_segmentWriter.hasIndexFor(column, ColumnIndexType.FST_INDEX)) {
            // Skip creating text index if already exists.
            LOGGER.info("Found text index for column: {}, in segment: {}", column, _segmentName);
            return;
        }

        LOGGER.info(
                "Creating new FST index for column: {} in segment: {}, hasDictionary: {}, cardinality: {}",
                column, _segmentName, hasDictionary, columnMetadata.getCardinality());
        File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(
                _indexDir, _segmentVersion);
        LuceneFSTIndexCreator luceneFSTIndexCreator =
             new LuceneFSTIndexCreator(
                     segmentDirectory, column,
                     columnMetadata.getCardinality(), null);
        try (BaseImmutableDictionary dictionary = getDictionaryReader(columnMetadata);) {
            for (int dictId = 0; dictId < dictionary.length(); dictId++) {
                luceneFSTIndexCreator.addSortedDictIds(
                        dictionary.getStringValue(dictId), dictId);
            }
        }
        luceneFSTIndexCreator.seal();
    }
}
