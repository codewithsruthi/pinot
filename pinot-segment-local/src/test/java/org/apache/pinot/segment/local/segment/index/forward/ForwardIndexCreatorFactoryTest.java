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

package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests [ForwardIndexCreatorFactory]'s forward-index encoding branch selection. Each test uses an isolated
/// temporary index directory and does not share mutable state.
public class ForwardIndexCreatorFactoryTest {
  private static final String COLUMN_NAME = "testCol";

  @Test
  public void testRawEncodingBuildsRawForwardIndexEvenWithDictionary()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
        new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW).build())) {
      assertFalse(creator.isDictionaryEncoded());
      assertEquals(creator.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);
      creator.putInt(1);
      creator.seal();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testCreatorReportsExplicitCompressionType()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .withCompressionType(ChunkCompressionType.SNAPPY)
        .build();
    try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
        config)) {
      assertEquals(creator.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.SNAPPY);
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testDefaultEncodingBuildsDictionaryForwardIndex()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try (ForwardIndexCreator creator =
        ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.DICTIONARY))) {
      assertTrue(creator.isDictionaryEncoded());
      creator.putDictId(0);
      creator.seal();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testTableConfigConstructorPropagatesCompressionStatsFlag()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
      tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);
      assertTrue(newContext(indexDir, true, tableConfig).isCompressionStatsEnabled());
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Any codecSpec, including a single compression stage, selects the self-describing V7 format.
  @Test
  public void testCompressionOnlyCodecSpecRoutesToV7Writer()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
          .withCodecSpec("LZ4")
          .build();
      try (ForwardIndexCreator creator =
          ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, false), config)) {
        assertFalse(creator.isDictionaryEncoded());
        assertNull(creator.getRawForwardIndexChunkCompressionType());
        creator.putInt(1);
        creator.seal();
      }
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      assertTrue(indexFile.exists());
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          ForwardIndexReader<?> reader =
              ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, DataType.INT, true)) {
        assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
            "codecSpec was routed to " + reader.getClass().getSimpleName());
        assertEquals(reader.getCodecSpec(), "LZ4");
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// A transform codecSpec is persisted in the V7 header and dispatched back to the V7 reader.
  @Test
  public void testTransformCodecSpecRoutesToV7Writer()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
          .withCodecSpec("DELTA,LZ4")
          .build();
      try (ForwardIndexCreator creator =
          ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, false), config)) {
        assertFalse(creator.isDictionaryEncoded());
        assertNull(creator.getRawForwardIndexChunkCompressionType(),
            "V7 codec-pipeline creator must not report a legacy compression type");
        creator.putInt(1);
        creator.seal();
      }
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      assertTrue(indexFile.exists());
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          ForwardIndexReader<?> reader =
              ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, DataType.INT, true)) {
        assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
            "Transform spec was routed to " + reader.getClass().getSimpleName());
        assertEquals(reader.getCodecSpec(), "DELTA,LZ4");
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// The V7 codec-pipeline writer only supports single-value columns, even for a simple compression spec.
  @Test
  public void testCompressionOnlyCodecSpecOnMvColumnThrows()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      assertThrows(IllegalArgumentException.class, () ->
          ForwardIndexCreatorFactory.createIndexCreator(newMvIntContext(indexDir),
               new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
                   .withCodecSpec("LZ4")
                   .build()));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// The V7 codec-pipeline writer only supports INT/LONG, even for a simple compression spec.
  @Test
  public void testCompressionOnlyCodecSpecOnStringColumnThrows()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      assertThrows(IllegalArgumentException.class, () ->
          ForwardIndexCreatorFactory.createIndexCreator(newStringContext(indexDir),
              new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
                  .withCodecSpec("SNAPPY")
                  .build()));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static IndexCreationContext newMvIntContext(File indexDir) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    return newContext(indexDir, false, tableConfig, new DimensionFieldSpec(COLUMN_NAME, DataType.INT, false));
  }

  private static IndexCreationContext newStringContext(File indexDir) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    return newContext(indexDir, false, tableConfig, new DimensionFieldSpec(COLUMN_NAME, DataType.STRING, true));
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    return newContext(indexDir, hasDictionary, tableConfig);
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary, TableConfig tableConfig) {
    return newContext(indexDir, hasDictionary, tableConfig,
        new DimensionFieldSpec(COLUMN_NAME, DataType.INT, true));
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary, TableConfig tableConfig,
      FieldSpec fieldSpec) {
    DataType storedType = fieldSpec.getDataType().getStoredType();
    int elementSize = storedType.isFixedWidth() ? storedType.size() : 8;
    ColumnMetadataImpl metadata = new ColumnMetadataImpl.Builder()
        .setFieldSpec(fieldSpec)
        .setTotalDocs(1)
        .setCardinality(2)
        .setHasDictionary(hasDictionary)
        .setLengthOfShortestElement(storedType.isFixedWidth() ? elementSize : 1)
        .setLengthOfLongestElement(elementSize)
        .setTotalNumberOfEntries(1)
        .setMaxNumberOfMultiValues(fieldSpec.isSingleValueField() ? 0 : 1)
        .build();
    return new IndexCreationContext.Builder(indexDir, tableConfig, metadata).build();
  }
}
