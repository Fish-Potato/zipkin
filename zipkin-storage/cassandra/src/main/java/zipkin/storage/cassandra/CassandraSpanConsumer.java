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
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
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
  private final PreparedStatement insertSampledTraceId;
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

    insertSampledTraceId = session.prepare(QueryBuilder
      .insertInto("zeus_sampled_trace")
      .value("service_name",QueryBuilder.bindMarker("serviceName"))
      .value("trace_id", QueryBuilder.bindMarker("traceId"))
      .value("bucket",QueryBuilder.bindMarker("bucket")));

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
    storeServiceTreeSample(rawSpans);
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

  /**
   * @Author onefish
   * add Zeus Plus
   */

  private final static Integer ZEUS_TTL = 604800000;

  private void storeServiceTreeSample(List<Span> spans) {
    if (CollectionUtils.isEmpty(spans)) {
      return ;
    }
    Long traceId = spans.get(0).traceId;
    for (Span span : spans) {
      String servicePath = getServicePath(span);
      String serviceName = guessZeusServiceName(span);
      if (!StringUtils.isEmpty(serviceName)) {
        if (SampleUtil.ifSample(StringUtils.isEmpty(servicePath)?serviceName:servicePath)) {
          storeSampledTrace(traceId, serviceName);
        }
      }
    }
  }

  /**
   * 当不是所有应用升级到zeus2.1 会导致rootService不准确
   * @param span
   * @return
   */
  private String getServicePath(Span span) {
    for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
      if ("servicePath".equals(binaryAnnotation.key) && null != binaryAnnotation.value) {
        String servicePath = new String(binaryAnnotation.value, Charset.forName("UTF-8"));
        if (!StringUtils.isEmpty(servicePath)) {
          return servicePath;
        }
      }
    }
    return null;
  }

  private String guessZeusServiceName(Span span) {
    if (null == span) {
      return "";
    }
    // 优先取serviceName
    for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
      if ("servicePath".equals(binaryAnnotation.key) && null != binaryAnnotation.value) {
        String servicePath = new String(binaryAnnotation.value, Charset.forName("UTF-8"));
        if (!StringUtils.isEmpty(servicePath)) {
          // 优先从header中获取服务名
          String serviceName;
          if (servicePath.contains("#")) {
            serviceName = servicePath.substring(servicePath.lastIndexOf("#"));
          } else {
            serviceName = servicePath;
          }
          if (!StringUtils.isEmpty(serviceName)) {
            return serviceName;
          }
        }
      }
    }

    // 然后取stackTrace中的ServiceName
    for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
      if (binaryAnnotation.key.equals("stackTrace")) {
        StackTrace stackTrace = JSON.parseObject(new String(binaryAnnotation.value, Charset.forName("UTF-8")), StackTrace.class);
        if (null != stackTrace.getServiceName() && stackTrace.getServiceName().length() > 0) {
          return stackTrace.getServiceName();
        }
      }
    }
    // 全部升级到2.1后 为了保证服务名的准确性，根span将不从url中取
    String serviceGroup = span.annotations.get(0).endpoint.serviceName;
    String serviceName = getFromUrl(span);
    if (!org.springframework.util.StringUtils.isEmpty(serviceName) && !serviceName.startsWith(serviceGroup)) {
      serviceName = serviceGroup + "." + serviceName;
    }
    return serviceName;
  }

  /**
   * 根据url获取服务名
   * @param span
   * @return
   */
  private String getFromUrl(Span span) {
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
    return "";
  }


  private void storeSampledTrace(Long traceId, String serviceName) {
    BoundStatement bound = CassandraUtil.bindWithName(insertSampledTraceId, "insert-sampled-trace")
      .setString("serviceName", serviceName)
      .setLong("traceId", traceId)
      .setLong("bucket", System.currentTimeMillis()/60000);
    session.executeAsync(bound);
  }

}
