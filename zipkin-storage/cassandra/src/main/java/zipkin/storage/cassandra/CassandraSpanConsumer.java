/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.cassandra;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ihomefnt.common.simhash.core.DefaultSimHashCalculator;
import com.ihomefnt.common.simhash.core.SimHashCalculator;
import com.ihomefnt.common.simhash.fingerprint.DefaultMd5FingerPrintCalculator;
import com.ihomefnt.common.simhash.tokenizer.DefaultSimHashTokenizer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.BinaryAnnotation;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.Nullable;
import zipkin.internal.Pair;
import zipkin.stack.StackTrace;
import zipkin.storage.guava.GuavaSpanConsumer;

import static com.google.common.util.concurrent.Futures.transform;
import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.storage.cassandra.CassandraUtil.bindWithName;

final class CassandraSpanConsumer implements GuavaSpanConsumer {
  private static final Executor executor = Executors.newCachedThreadPool();
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSpanConsumer.class);
  private static final long WRITTEN_NAMES_TTL
      = Long.getLong("zipkin.store.cassandra.internal.writtenNamesTtl", 60 * 60 * 1000);

  private static final Function<Object, Void> TO_VOID = Functions.<Void>constant(null);

  private final Session session;
  private final TimestampCodec timestampCodec;
  @Deprecated
  private final int spanTtl;
  @Deprecated
  private final Integer indexTtl;
  private final PreparedStatement insertSpan;
  private final PreparedStatement insertServiceName;
  private final PreparedStatement insertSpanName;
  private final PreparedStatement insertServiceTreeRoot;
  private final PreparedStatement insertServiceTree;
  private final Schema.Metadata metadata;
  private final DeduplicatingExecutor deduplicatingExecutor;
  private final CompositeIndexer indexer;


  CassandraSpanConsumer(Session session, int bucketCount, int spanTtl, int indexTtl,
      @Nullable CacheBuilderSpec indexCacheSpec) {
    this.session = session;
    this.timestampCodec = new TimestampCodec(session);
    this.spanTtl = spanTtl;
    this.metadata = Schema.readMetadata(session);
    this.indexTtl = metadata.hasDefaultTtl ? null : indexTtl;
    insertSpan = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("traces")
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("span_name", QueryBuilder.bindMarker("span_name"))
            .value("span", QueryBuilder.bindMarker("span"))));

    insertServiceName = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto(Tables.SERVICE_NAMES)
            .value("service_name", QueryBuilder.bindMarker("service_name"))));

    insertSpanName = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto(Tables.SPAN_NAMES)
            .value("service_name", QueryBuilder.bindMarker("service_name"))
            .value("bucket", 0) // bucket is deprecated on this index
            .value("span_name", QueryBuilder.bindMarker("span_name"))));

    insertServiceTreeRoot = session.prepare(
      maybeUseTtl(QueryBuilder
        .insertInto("root_tree")
        .value("key",QueryBuilder.bindMarker("key"))
        .value("root_service", QueryBuilder.bindMarker("root_service"))
        .value("service_tree", QueryBuilder.bindMarker("service_tree"))));

    insertServiceTree = session.prepare(
      maybeUseTtl(QueryBuilder
        .insertInto("service_call")
        .value("service_name",QueryBuilder.bindMarker("service_name"))
        .value("parent_service", QueryBuilder.bindMarker("parent_service"))
        .value("root_service", QueryBuilder.bindMarker("root_service"))));

    deduplicatingExecutor = new DeduplicatingExecutor(session, WRITTEN_NAMES_TTL);
    indexer = new CompositeIndexer(session, indexCacheSpec, bucketCount, this.indexTtl);
  }

  private RegularStatement maybeUseTtl(Insert value) {
    return indexTtl == null
        ? value
        : value.using(QueryBuilder.ttl(QueryBuilder.bindMarker("ttl_")));
  }

  /**
   * This fans out into many requests, last count was 8 * spans.size. If any of these fail, the
   * returned future will fail. Most callers drop or log the result.
   */
  @Override
  public ListenableFuture<Void> accept(List<Span> rawSpans) {
    /**
     * 暂时不存这部分数据
     */
//    storeServiceCallTree(rawSpans);
    ImmutableSet.Builder<ListenableFuture<?>> futures = ImmutableSet.builder();

    ImmutableList.Builder<Span> spans = ImmutableList.builder();
    for (Span span : rawSpans) {
      // indexing occurs by timestamp, so derive one if not present.
      Long timestamp = guessTimestamp(span);
      spans.add(span);

      futures.add(storeSpan(
          span.traceId,
          timestamp != null ? timestamp : 0L,
          String.format("%s%d_%d_%d",
              span.traceIdHigh == 0 ? "" : span.traceIdHigh + "_",
              span.id,
              span.annotations.hashCode(),
              span.binaryAnnotations.hashCode()),
          // store the raw span without any adjustments
          ByteBuffer.wrap(Codec.THRIFT.writeSpan(span))));

      for (String serviceName : span.serviceNames()) {
        // SpanStore.getServiceNames
        futures.add(storeServiceName(serviceName));
        if (!span.name.isEmpty()) {
          // SpanStore.getSpanNames
          futures.add(storeSpanName(serviceName, span.name));
        }
      }
    }
    futures.addAll(indexer.index(spans.build()));
    return transform(Futures.allAsList(futures.build()), TO_VOID);
  }

  /**
   * #####################################
   */
  private void storeServiceCallTree(List<Span> sampled) {
    List<Span> spanList = new ArrayList<>(sampled);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        ServiceCallTree root = buildServiceTree(spanList);
        if (null != root) {
          storeCallTreeRoot(root);
          storeCallTreeNode(root.getChildren(), root, root);
        }
      }

      private void storeCallTreeNode(List<ServiceCallTree> children, ServiceCallTree parent, ServiceCallTree root) {
        if (null == children || children.size() <= 0) {
          return;
        }
        // store service relation
        for (ServiceCallTree child : children) {
          BoundStatement bound = CassandraUtil.bindWithName(insertServiceTree, "insert-tree-node")
            .setString("service_name", child.getServiceName())
            .setString("parent_service", parent.getServiceName())
            .setString("root_service", root.getServiceName());
          if (!metadata.hasDefaultTtl) bound.setInt("ttl_", spanTtl);
          deduplicatingExecutor.maybeExecuteAsync(bound, child.getServiceName() + "."
            + parent.getServiceName() + "." + root.getServiceName());
          // 递归存储子节点
          storeCallTreeNode(child.getChildren(), child, root);
        }
      }

      /**
       * @param root
       */
      private void storeCallTreeRoot(ServiceCallTree root) {
        SimHashCalculator calculator = DefaultSimHashCalculator.DefaultSimHashCalculatorBuilder.aDefaultSimHashCalculator()
          .fingerPrintCalculator(new DefaultMd5FingerPrintCalculator())
          .simHashTokenizer(new DefaultSimHashTokenizer())
          .build();

        String serviceTreeJson = JSON.toJSONString(root);
        String key = calculator.getSimHash(serviceTreeJson);
        // store root
        BoundStatement bound = CassandraUtil.bindWithName(insertServiceTreeRoot, "insert-tree-root")
          .setString("key", key)
          .setString("root_service", root.getServiceName())
          .setString("service_tree", serviceTreeJson);

        BoundStatement nodeBound = CassandraUtil.bindWithName(insertServiceTree, "insert-tree-node")
          .setString("service_name", root.getServiceName())
          .setString("parent_service", "")
          .setString("root_service", "");
        if (!metadata.hasDefaultTtl) bound.setInt("ttl_", spanTtl);
        if (StringUtils.isNotEmpty(key)) deduplicatingExecutor.maybeExecuteAsync(bound, key);
        if (StringUtils.isNotEmpty(root.getServiceName())) deduplicatingExecutor.maybeExecuteAsync(nodeBound, root.getServiceName());
      }
    });
  }

  private ServiceCallTree buildServiceTree(List<Span> sampled) {
    Span rootSpan = null;
    for (Span span : sampled) {
      if (span.parentId == null) {
        rootSpan =span;
        break;
      }
    }
    if (null != rootSpan) {
      ServiceCallTree root = new ServiceCallTree();
      root.setServiceName(buildServiceName(rootSpan));
      root.setSpanId(rootSpan.id);
      sampled.remove(rootSpan);
      buildChildren(root, sampled);
      return root;
    }

    return null;
  }

  private String buildServiceName(Span span) {
    // 优先从stackTrace中取serviceName
    for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
      if (binaryAnnotation.key.equals("stackTrace")) {
        StackTrace stackTrace = JSON.parseObject(new String(binaryAnnotation.value, Charset.forName("UTF-8")), StackTrace.class);
        if (null != stackTrace.getServiceName() && stackTrace.getServiceName().length() > 0) {
          return stackTrace.getServiceName();
        }
      }
    }

    for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
      if ("http.url".equals(binaryAnnotation.key)) {
        String value = new String(binaryAnnotation.value, Charset.forName("UTF-8"));
        Pattern urlPathVariablePattern = Pattern.compile(".*/[0-9]+/.*");
        Matcher matcher = urlPathVariablePattern.matcher(value);
        if (matcher.matches()) {
          value = value.split("/[0-9]+/")[0];
        }
        if (!value.startsWith("http://")) {
          value = value.replace("/",".");
          if (value.startsWith(".")) {
            return value.substring(value.indexOf(".") + 1);
          }
          return value;
        }
      }
    }

    for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
      if ("http.url".equals(binaryAnnotation.key)) {
        String value = new String(binaryAnnotation.value, Charset.forName("UTF-8"));
        Pattern urlPathVariablePattern = Pattern.compile(".*/[0-9]+/.*");
        Matcher matcher = urlPathVariablePattern.matcher(value);
        if (matcher.matches()) {
          value = value.split("/[0-9]+/")[0];
        }
        value = value.replaceAll("http://[^/]+/","");
        return value.replace("/",".");
      }
    }
    return null;
  }


  private void buildChildren(ServiceCallTree serviceCallTree, List<Span> spans) {
    if (spans == null || spans.size() == 0) {
      return ;
    }
    List<Span> removeList = new ArrayList<>();
    for (Span span : spans) {
      if (null != span.parentId && span.parentId.equals(serviceCallTree.getSpanId())) {
        serviceCallTree.addChild(span);
        removeList.add(span);
      }
    }

    if (removeList.size()==0) {
      // 已到叶子节点
      serviceCallTree.setLeaf(true);
      return ;
    }
    // 去除已经构造过的span
    spans.removeAll(removeList);

    // 递归生成树
    for (ServiceCallTree tree : serviceCallTree.getChildren()) {
      buildChildren(tree, spans);
    }

  }


   class ServiceCallTree {

    @JSONField(serialize = false,deserialize = false)
    private Long spanId;

    private String serviceName;

    private boolean leaf;

    private List<ServiceCallTree> children = new ArrayList<>();

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    public List<ServiceCallTree> getChildren() {
      return children;
    }

    public Long getSpanId() {
      return spanId;
    }

    public void setSpanId(Long spanId) {
      this.spanId = spanId;
    }

    public void addChild(Span span) {
      String serviceName = buildServiceName(span);
      if (StringUtils.isNotEmpty(serviceName)) {
        ServiceCallTree child = new ServiceCallTree();
        child.setSpanId(span.id);
        child.setServiceName(serviceName);
        children.add(child);
      }
    }

    public boolean isLeaf() {
      return leaf;
    }

    public void setLeaf(boolean leaf) {
      this.leaf = leaf;
    }
  }

  /**
   * Store the span in the underlying storage for later retrieval.
   */
  ListenableFuture<?> storeSpan(long traceId, long timestamp, String key, ByteBuffer span) {
    try {
      // If we couldn't guess the timestamp, that probably means that there was a missing timestamp.
      if (0 == timestamp && metadata.compactionClass.contains("DateTieredCompactionStrategy")) {
        LOG.warn("Span {} in trace {} had no timestamp. "
            + "If this happens a lot consider switching back to SizeTieredCompactionStrategy for "
            + "{}.traces", key, traceId, session.getLoggedKeyspace());
      }

      BoundStatement bound = bindWithName(insertSpan, "insert-span")
          .setLong("trace_id", traceId)
          .setBytesUnsafe("ts", timestampCodec.serialize(timestamp))
          .setString("span_name", key)
          .setBytes("span", span);
      if (!metadata.hasDefaultTtl) bound.setInt("ttl_", spanTtl);

      return session.executeAsync(bound);
    } catch (RuntimeException ex) {
      return Futures.immediateFailedFuture(ex);
    }
  }

  ListenableFuture<?> storeServiceName(final String serviceName) {
    BoundStatement bound = bindWithName(insertServiceName, "insert-service-name")
        .setString("service_name", serviceName);
    if (indexTtl != null) bound.setInt("ttl_", indexTtl);
    return deduplicatingExecutor.maybeExecuteAsync(bound, serviceName);
  }

  ListenableFuture<?> storeSpanName(String serviceName, String spanName) {
    BoundStatement bound = bindWithName(insertSpanName, "insert-span-name")
        .setString("service_name", serviceName)
        .setString("span_name", spanName);
    if (indexTtl != null) bound.setInt("ttl_", indexTtl);
    return deduplicatingExecutor.maybeExecuteAsync(bound, Pair.create(serviceName, spanName));
  }

  /** Clears any caches */
  @VisibleForTesting void clear() {
    indexer.clear();
    deduplicatingExecutor.clear();
  }
}
