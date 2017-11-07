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
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import zipkin.*;
import zipkin.internal.CorrectForClockSkew;
import zipkin.internal.Dependencies;
import zipkin.internal.DependencyLinker;
import zipkin.internal.GroupByTraceId;
import zipkin.internal.MergeById;
import zipkin.internal.Nullable;
import zipkin.stack.StackTrace;
import zipkin.storage.QueryRequest;
import zipkin.storage.guava.GuavaSpanStore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static zipkin.internal.Util.getDays;

public final class CassandraSpanStore implements GuavaSpanStore {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSpanStore.class);

  static final ListenableFuture<List<String>> EMPTY_LIST =
      immediateFuture(Collections.<String>emptyList());

  private final int maxTraceCols;
  private final int indexFetchMultiplier;
  private final boolean strictTraceId;
  private final Session session;
  private final TimestampCodec timestampCodec;
  private final Set<Integer> buckets;
  private final PreparedStatement selectTraces;
  private final PreparedStatement selectDependencies;
  private final PreparedStatement selectServiceNames;
  private final PreparedStatement selectSpanNames;
  private final PreparedStatement selectTraceIdsByServiceName;
  private final PreparedStatement selectTraceIdsByServiceNames;
  private final PreparedStatement selectTraceIdsBySpanName;
  private final PreparedStatement selectTraceIdsByAnnotation;
  private final Function<ResultSet, Map<Long, Long>> traceIdToTimestamp;
  private final DeduplicatingExecutor deduplicatingExecutor;
  private final PreparedStatement insertServiceTree;
  private final PreparedStatement updateServiceTree;
  private final PreparedStatement insertServiceTreeNode;
  private final PreparedStatement selectServiceTree;

  CassandraSpanStore(Session session, int bucketCount, int maxTraceCols, int indexFetchMultiplier,
      boolean strictTraceId) {
    this.session = session;
    this.maxTraceCols = maxTraceCols;
    this.indexFetchMultiplier = indexFetchMultiplier;
    this.strictTraceId = strictTraceId;

    ProtocolVersion protocolVersion = session.getCluster()
        .getConfiguration().getProtocolOptions().getProtocolVersion();
    this.timestampCodec = new TimestampCodec(protocolVersion);
    this.buckets = ContiguousSet.create(Range.closedOpen(0, bucketCount), integers());

    selectTraces = session.prepare(
        QueryBuilder.select("trace_id", "span")
            .from("traces")
            .where(QueryBuilder.in("trace_id", QueryBuilder.bindMarker("trace_id")))
            .limit(QueryBuilder.bindMarker("limit_")));

    selectDependencies = session.prepare(
        QueryBuilder.select("dependencies")
            .from("dependencies")
            .where(QueryBuilder.in("day", QueryBuilder.bindMarker("days"))));

    selectServiceNames = session.prepare(
        QueryBuilder.select("service_name")
            .from(Tables.SERVICE_NAMES));

    selectSpanNames = session.prepare(
        QueryBuilder.select("span_name")
            .from(Tables.SPAN_NAMES)
            .where(QueryBuilder.eq("service_name", QueryBuilder.bindMarker("service_name")))
            .and(QueryBuilder.eq("bucket", QueryBuilder.bindMarker("bucket")))
            .limit(QueryBuilder.bindMarker("limit_")));

    selectTraceIdsByServiceName = session.prepare(
        QueryBuilder.select("ts", "trace_id")
            .from(Tables.SERVICE_NAME_INDEX)
            .where(QueryBuilder.eq("service_name", QueryBuilder.bindMarker("service_name")))
            .and(QueryBuilder.in("bucket", QueryBuilder.bindMarker("bucket")))
            .and(QueryBuilder.gte("ts", QueryBuilder.bindMarker("start_ts")))
            .and(QueryBuilder.lte("ts", QueryBuilder.bindMarker("end_ts")))
            .limit(QueryBuilder.bindMarker("limit_"))
            .orderBy(QueryBuilder.desc("ts")));

    selectTraceIdsBySpanName = session.prepare(
        QueryBuilder.select("ts", "trace_id")
            .from(Tables.SERVICE_SPAN_NAME_INDEX)
            .where(
                QueryBuilder.eq("service_span_name", QueryBuilder.bindMarker("service_span_name")))
            .and(QueryBuilder.gte("ts", QueryBuilder.bindMarker("start_ts")))
            .and(QueryBuilder.lte("ts", QueryBuilder.bindMarker("end_ts")))
            .limit(QueryBuilder.bindMarker("limit_"))
            .orderBy(QueryBuilder.desc("ts")));

    selectTraceIdsByAnnotation = session.prepare(
        QueryBuilder.select("ts", "trace_id")
            .from(Tables.ANNOTATIONS_INDEX)
            .where(QueryBuilder.eq("annotation", QueryBuilder.bindMarker("annotation")))
            .and(QueryBuilder.in("bucket", QueryBuilder.bindMarker("bucket")))
            .and(QueryBuilder.gte("ts", QueryBuilder.bindMarker("start_ts")))
            .and(QueryBuilder.lte("ts", QueryBuilder.bindMarker("end_ts")))
            .limit(QueryBuilder.bindMarker("limit_"))
            .orderBy(QueryBuilder.desc("ts")));

    if (protocolVersion.compareTo(ProtocolVersion.V4) < 0) {
      LOG.warn("Please update Cassandra to 2.2 or later, as some features may fail");
      // Log vs failing on "Partition KEY part service_name cannot be restricted by IN relation"
      selectTraceIdsByServiceNames = null;
    } else {
      selectTraceIdsByServiceNames = session.prepare(
          QueryBuilder.select("ts", "trace_id")
              .from(Tables.SERVICE_NAME_INDEX)
              .where(QueryBuilder.in("service_name", QueryBuilder.bindMarker("service_name")))
              .and(QueryBuilder.in("bucket", QueryBuilder.bindMarker("bucket")))
              .and(QueryBuilder.gte("ts", QueryBuilder.bindMarker("start_ts")))
              .and(QueryBuilder.lte("ts", QueryBuilder.bindMarker("end_ts")))
              .limit(QueryBuilder.bindMarker("limit_"))
              .orderBy(QueryBuilder.desc("ts")));
    }

    traceIdToTimestamp = input -> {
      Map<Long, Long> result = new LinkedHashMap<>();
      for (Row row : input) {
        result.put(row.getLong("trace_id"), timestampCodec.deserialize(row, "ts"));
      }
      return result;
    };

    insertServiceTree = session.prepare(
      useTtl(QueryBuilder
        .insertInto("zeus_service_tree")
        .value("tag",QueryBuilder.bindMarker("tag"))
        .value("node_path", QueryBuilder.bindMarker("nodePath"))));

    insertServiceTreeNode = session.prepare(
      useTtl(QueryBuilder
        .insertInto("zeus_service_node")
        .value("service_name",QueryBuilder.bindMarker("serviceName"))
        .value("parent_service", QueryBuilder.bindMarker("parentService"))
        .value("root_service", QueryBuilder.bindMarker("rootService"))
        .value("tag", QueryBuilder.bindMarker("tag"))));
    deduplicatingExecutor = new DeduplicatingExecutor(session, 3600000);

    selectServiceTree = session.prepare(
      QueryBuilder.select("tag", "node_path")
        .from("zeus_service_tree")
        .where(QueryBuilder.eq("tag", QueryBuilder.bindMarker("tag"))));

    updateServiceTree = session.prepare(QueryBuilder.update("zeus_service_tree")
      .with(QueryBuilder.addAll("node_path",QueryBuilder.bindMarker("nodePath")))
      .where(QueryBuilder.eq("tag",QueryBuilder.bindMarker("tag"))));

    executeStoreTree();
  }

  private RegularStatement useTtl(Insert value) {
    return value.using(QueryBuilder.ttl(QueryBuilder.bindMarker("ttl_")));
  }

  /**
   * This fans out into a potentially large amount of requests related to the amount of annotations
   * queried. The returned future will fail if any of the inputs fail.
   *
   * <p>When {@link QueryRequest#serviceName service name} is unset, service names will be
   * fetched eagerly, implying an additional query.
   */
  @Override
  public ListenableFuture<List<List<Span>>> getTraces(final QueryRequest request) {
    // Over fetch on indexes as they don't return distinct (trace id, timestamp) rows.
    final int traceIndexFetchSize = request.limit * indexFetchMultiplier;
    ListenableFuture<Map<Long, Long>> traceIdToTimestamp;
    if (request.spanName != null) {
      traceIdToTimestamp = getTraceIdsBySpanName(request.serviceName, request.spanName,
          request.endTs * 1000, request.lookback * 1000, traceIndexFetchSize);
    } else if (request.serviceName != null) {
      traceIdToTimestamp = getTraceIdsByServiceNames(Collections.singletonList(request.serviceName),
          request.endTs * 1000, request.lookback * 1000, traceIndexFetchSize);
    } else {
      checkArgument(selectTraceIdsByServiceNames != null,
          "getTraces without serviceName requires Cassandra 2.2 or later");
      traceIdToTimestamp = transform(getServiceNames(),
          new AsyncFunction<List<String>, Map<Long, Long>>() {
            @Override public ListenableFuture<Map<Long, Long>> apply(List<String> serviceNames) {
              return getTraceIdsByServiceNames(serviceNames,
                  request.endTs * 1000, request.lookback * 1000, traceIndexFetchSize);
            }
          });
    }

    List<String> annotationKeys = CassandraUtil.annotationKeys(request);

    ListenableFuture<Set<Long>> traceIds;
    if (annotationKeys.isEmpty()) {
      // Simplest case is when there is no annotation query. Limit is valid since there's no AND
      // query that could reduce the results returned to less than the limit.
      traceIds = Futures.transform(traceIdToTimestamp, CassandraUtil.keyset());
    } else {
      // While a valid port of the scala cassandra span store (from zipkin 1.35), there is a fault.
      // each annotation key is an intersection, meaning we likely return < traceIndexFetchSize.
      List<ListenableFuture<Map<Long, Long>>> futureKeySetsToIntersect = new ArrayList<>();
      if (request.spanName != null) {
        futureKeySetsToIntersect.add(traceIdToTimestamp);
      }
      for (String annotationKey : annotationKeys) {
        futureKeySetsToIntersect.add(getTraceIdsByAnnotation(annotationKey,
            request.endTs * 1000, request.lookback * 1000, traceIndexFetchSize));
      }
      // We achieve the AND goal, by intersecting each of the key sets.
      traceIds = Futures.transform(allAsList(futureKeySetsToIntersect), CassandraUtil.intersectKeySets());
    }
    return transform(traceIds, new AsyncFunction<Set<Long>, List<List<Span>>>() {
      @Override public ListenableFuture<List<List<Span>>> apply(Set<Long> traceIds) {
        traceIds = ImmutableSet.copyOf(Iterators.limit(traceIds.iterator(), request.limit));
        return transform(getSpansByTraceIds(traceIds, maxTraceCols),
            new Function<List<Span>, List<List<Span>>>() {
              @Override public List<List<Span>> apply(List<Span> input) {
                // Indexes only contain Span.traceId, so our matches are imprecise on Span.traceIdHigh
                return FluentIterable.from(GroupByTraceId.apply(input, strictTraceId, true))
                    .filter(trace -> trace.get(0).traceIdHigh == 0 || request.test(trace))
                    .toList();
              }
            });
      }

      @Override public String toString() {
        return "getSpansByTraceIds";
      }
    });
  }

  @Override public ListenableFuture<List<Span>> getRawTrace(long traceId) {
    return getRawTrace(0L, traceId);
  }

  /**
   * Since the schema doesn't have a unique index on {@link Span#traceIdHigh}, we have to filter
   * client-side.
   */
  @Override public ListenableFuture<List<Span>> getRawTrace(final long traceIdHigh, long traceIdLow) {
    return transform(getSpansByTraceIds(Collections.singleton(traceIdLow), maxTraceCols),
        new Function<List<Span>, List<Span>>() {
          @Override public List<Span> apply(List<Span> input) {
            if (strictTraceId) {
              Iterator<Span> spans = input.iterator();
              while (spans.hasNext()) {
                long nextTraceIdHigh = spans.next().traceIdHigh;
                if (nextTraceIdHigh != 0L && nextTraceIdHigh != traceIdHigh) {
                  spans.remove();
                }
              }
            }
            return input.isEmpty() ? null : input;
          }
        });
  }

  @Override public ListenableFuture<List<Span>> getTrace(long traceId) {
    return getTrace(0L, traceId);
  }

  @Override public ListenableFuture<List<Span>> getTrace(long traceIdHigh, long traceIdLow) {
    return transform(getRawTrace(traceIdHigh, traceIdLow), AdjustTrace.INSTANCE);
  }

  enum AdjustTrace implements Function<Collection<Span>, List<Span>> {
    INSTANCE;

    @Override public List<Span> apply(Collection<Span> input) {
      List<Span> result = CorrectForClockSkew.apply(MergeById.apply(input));
      return result.isEmpty() ? null : result;
    }
  }

  @Override public ListenableFuture<List<String>> getServiceNames() {
    try {
      BoundStatement bound = CassandraUtil.bindWithName(selectServiceNames, "select-service-names");
      return transform(session.executeAsync(bound), new Function<ResultSet, List<String>>() {
            @Override public List<String> apply(ResultSet input) {
              Set<String> serviceNames = new HashSet<>();
              for (Row row : input) {
                serviceNames.add(row.getString("service_name"));
              }
              return Ordering.natural().sortedCopy(serviceNames);
            }
          }
      );
    } catch (RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  @Override public ListenableFuture<List<String>> getSpanNames(String serviceName) {
    if (serviceName == null || serviceName.isEmpty()) return EMPTY_LIST;
    serviceName = checkNotNull(serviceName, "serviceName").toLowerCase();
    int bucket = 0;
    try {
      BoundStatement bound = CassandraUtil.bindWithName(selectSpanNames, "select-span-names")
          .setString("service_name", serviceName)
          .setInt("bucket", bucket)
          // no one is ever going to browse so many span names
          .setInt("limit_", 1000);

      return transform(session.executeAsync(bound), new Function<ResultSet, List<String>>() {
            @Override public List<String> apply(ResultSet input) {
              Set<String> spanNames = new HashSet<>();
              for (Row row : input) {
                spanNames.add(row.getString("span_name"));
              }
              return Ordering.natural().sortedCopy(spanNames);
            }
          }
      );
    } catch (RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  @Override public ListenableFuture<List<DependencyLink>> getDependencies(long endTs,
      @Nullable Long lookback) {
    List<Date> days = getDays(endTs, lookback);
    try {
      BoundStatement bound = CassandraUtil.bindWithName(selectDependencies, "select-dependencies")
          .setList("days", days);
      return transform(session.executeAsync(bound), ConvertDependenciesResponse.INSTANCE);
    } catch (RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  enum ConvertDependenciesResponse implements Function<ResultSet, List<DependencyLink>> {
    INSTANCE;

    @Override public List<DependencyLink> apply(ResultSet rs) {
      ImmutableList.Builder<DependencyLink> unmerged = ImmutableList.builder();
      for (Row row : rs) {
        ByteBuffer encodedDayOfDependencies = row.getBytes("dependencies");
        for (DependencyLink link : Dependencies.fromThrift(encodedDayOfDependencies).links) {
          unmerged.add(link);
        }
      }
      return DependencyLinker.merge(unmerged.build());
    }
  }

  /**
   * Get the available trace information from the storage system. Spans in trace should be sorted by
   * the first annotation timestamp in that span. First event should be first in the spans list. <p>
   * The return list will contain only spans that have been found, thus the return list may not
   * match the provided list of ids.
   */
  ListenableFuture<List<Span>> getSpansByTraceIds(Set<Long> traceIds, int limit) {
    checkNotNull(traceIds, "traceIds");
    if (traceIds.isEmpty()) {
      return immediateFuture(Collections.<Span>emptyList());
    }

    try {
      BoundStatement bound = CassandraUtil.bindWithName(selectTraces, "select-traces")
          .setSet("trace_id", traceIds)
          .setInt("limit_", limit);

      bound.setFetchSize(Integer.MAX_VALUE);

      return transform(session.executeAsync(bound),
          new Function<ResultSet, List<Span>>() {
            @Override public List<Span> apply(ResultSet input) {
              List<Span> result = new ArrayList<>(input.getAvailableWithoutFetching());
              for (Row row : input) {
                result.add(Codec.THRIFT.readSpan(row.getBytes("span")));
              }
              return result;
            }
          }
      );
    } catch (RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  ListenableFuture<Map<Long, Long>> getTraceIdsByServiceNames(List<String> serviceNames, long endTs,
      long lookback, int limit) {
    if (serviceNames.isEmpty()) return immediateFuture(Collections.<Long, Long>emptyMap());

    long startTs = Math.max(endTs - lookback, 0); // >= 1970
    try {
      // This guards use of "in" query to give people a little more time to move off Cassandra 2.1
      // Note that it will still fail when serviceNames.size() > 1
      BoundStatement bound = serviceNames.size() == 1 ?
          CassandraUtil.bindWithName(selectTraceIdsByServiceName, "select-trace-ids-by-service-name")
              .setString("service_name", serviceNames.get(0))
              .setSet("bucket", buckets)
              .setBytesUnsafe("start_ts", timestampCodec.serialize(startTs))
              .setBytesUnsafe("end_ts", timestampCodec.serialize(endTs))
              .setInt("limit_", limit) :
          CassandraUtil.bindWithName(selectTraceIdsByServiceNames, "select-trace-ids-by-service-names")
              .setList("service_name", serviceNames)
              .setSet("bucket", buckets)
              .setBytesUnsafe("start_ts", timestampCodec.serialize(startTs))
              .setBytesUnsafe("end_ts", timestampCodec.serialize(endTs))
              .setInt("limit_", limit);

      bound.setFetchSize(Integer.MAX_VALUE);

      return transform(session.executeAsync(bound), traceIdToTimestamp);
    } catch (RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  ListenableFuture<Map<Long, Long>> getTraceIdsBySpanName(String serviceName,
      String spanName, long endTs, long lookback, int limit) {
    checkArgument(serviceName != null, "serviceName required on spanName query");
    checkArgument(spanName != null, "spanName required on spanName query");
    String serviceSpanName = serviceName + "." + spanName;
    long startTs = Math.max(endTs - lookback, 0); // >= 1970
    try {
      BoundStatement bound = CassandraUtil.bindWithName(selectTraceIdsBySpanName, "select-trace-ids-by-span-name")
          .setString("service_span_name", serviceSpanName)
          .setBytesUnsafe("start_ts", timestampCodec.serialize(startTs))
          .setBytesUnsafe("end_ts", timestampCodec.serialize(endTs))
          .setInt("limit_", limit);

      return transform(session.executeAsync(bound), traceIdToTimestamp);
    } catch (RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  ListenableFuture<Map<Long, Long>> getTraceIdsByAnnotation(String annotationKey,
      long endTs, long lookback, int limit) {
    long startTs = Math.max(endTs - lookback, 0); // >= 1970
    try {
      BoundStatement bound =
          CassandraUtil.bindWithName(selectTraceIdsByAnnotation, "select-trace-ids-by-annotation")
              .setBytes("annotation", CassandraUtil.toByteBuffer(annotationKey))
              .setSet("bucket", buckets)
              .setBytesUnsafe("start_ts", timestampCodec.serialize(startTs))
              .setBytesUnsafe("end_ts", timestampCodec.serialize(endTs))
              .setInt("limit_", limit);

      bound.setFetchSize(Integer.MAX_VALUE);

      return transform(session.executeAsync(bound), new Function<ResultSet, Map<Long, Long>>() {
            @Override public Map<Long, Long> apply(ResultSet input) {
              Map<Long, Long> traceIdsToTimestamps = new LinkedHashMap<>();
              for (Row row : input) {
                traceIdsToTimestamps.put(row.getLong("trace_id"),
                    timestampCodec.deserialize(row, "ts"));
              }
              return traceIdsToTimestamps;
            }
          }
      );
    } catch (CharacterCodingException | RuntimeException ex) {
      return immediateFailedFuture(ex);
    }
  }

  /**
   * @Author onefish
   * add Zeus Plus
   */

  private final static Integer ZEUS_TTL = 604800000;

  private ListeningScheduledExecutorService scheduledExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());


  private void executeStoreTree() {
    // 避免数据还未录入，减去60秒的误差
    long endTs = System.currentTimeMillis()-60000;
    scheduledExecutor.scheduleAtFixedRate(new ZeusServiceRunnable(endTs,1000)
      , 10, 1, TimeUnit.SECONDS);
  }

  class ZeusServiceRunnable implements Runnable {

    private long lookBack;

    private long endTs;

    public ZeusServiceRunnable(long endTs, long lookBack) {
      this.endTs = endTs;
      this.lookBack = lookBack;
    }

    @Override
    public void run() {
      QueryRequest.Builder builder = QueryRequest.builder();
      builder.lookback(lookBack).endTs(endTs);
      try {
        List<List<Span>> spansList = getTraces(builder.build()).get();
        for (List<Span> spans : spansList) {
          storeServiceTree(spans);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }


    private void storeServiceTree(List<Span> spans) {
      if (CollectionUtils.isEmpty(spans)) {
        return ;
      }
      Map<Long,Span> spanMap = new HashMap<>();
      Set<Long> parentIdSet = Sets.newHashSet();
      for (Span span : spans) {
        spanMap.put(span.id,span);
        if (null != span.parentId) {
          if (!isSqlQuery(span))
          parentIdSet.add(span.parentId);
        }
      }
      Long traceId = spans.get(0).traceId;
      Span rootSpan = spanMap.get(traceId);
      String rootService = getZeusServiceName(rootSpan);
      if (StringUtils.isEmpty(rootService)) {
        return ;
      }
      List<String> treePathList = Lists.newArrayList();
      List<ZeusServiceTreeNode> nodeList = Lists.newArrayList();
      Set<String> existHelper = Sets.newHashSet();
      for (Span span : spans) {
        // 叶节点
        if (!parentIdSet.contains(span.id) && !isSqlQuery(span)) {
          String treePath = buildTreePath(span, spanMap);
          treePathList.add(treePath);
        }
        String serviceName = getZeusServiceName(span);
        if (StringUtils.isEmpty(serviceName)) {
          continue;
        }
        String parentServiceName = getZeusServiceName(spanMap.get(span.parentId));
        if (!existHelper.contains(serviceName+"->"+parentServiceName)) {
          ZeusServiceTreeNode treeNode = new ZeusServiceTreeNode();
          treeNode.serviceName = serviceName;
          treeNode.parentService = parentServiceName;
          treeNode.rootService = rootService;
          existHelper.add(serviceName+"->"+parentServiceName);
          nodeList.add(treeNode);
        }
      }
      if (CollectionUtils.isEmpty(treePathList)) {
        return ;
      }

      String[] treeTag = calculateTreeTag(treePathList);
      for (ZeusServiceTreeNode zeusServiceTreeNode : nodeList) {
        zeusServiceTreeNode.treeTag = treeTag[0];
        storeTreeNode(zeusServiceTreeNode);
      }

      try {
        storeTreeTag(treeTag[0], treeTag[1]);
      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private boolean isSqlQuery(Span span) {
      for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
        if (binaryAnnotation.key.equals("sql.query")) {
          return true;
        }
      }
      return false;
    }

    private String[] calculateTreeTag(List<String> treePathList) {
      Map<String,Integer> treePathCount = new HashMap<>();
      for (String treePath : treePathList) {
        int count = 1;
        if (treePathCount.containsKey(treePath)) {
          count = treePathCount.get(treePath) + 1;
        }
        treePathCount.put(treePath, count);
      }
      List<String> sortedTreePath = Lists.newArrayList(treePathCount.keySet());
      Collections.sort(sortedTreePath);
      StringBuilder tree = new StringBuilder();
      StringBuilder tag = new StringBuilder();
      for (String treePath : sortedTreePath) {
        int count = treePathCount.get(treePath);
        tree.append(treePath).append("#").append(count > 1 ? "n" : 1).append(";");
        tag.append(treePath).append(";");
      }
      return new String[]{tag.toString(),tree.toString()};
    }

    private String buildTreePath(Span currentSpan, Map<Long, Span> spanMap) {
      Long parentId = currentSpan.parentId;
      StringBuilder servicePath = new StringBuilder(getZeusServiceName(currentSpan));
      while(true) {
        if (null == parentId || !spanMap.containsKey(parentId)) {
          break;
        }
        Span parentSpan = spanMap.get(parentId);
        parentId = parentSpan.parentId;
        servicePath.insert(0, getZeusServiceName(parentSpan) + "=>");
      }
      return servicePath.toString();
    }

    private String getZeusServiceName(Span span) {
      if (null == span) {
        return "";
      }
      // 优先取serviceName
      for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
        if (binaryAnnotation.key.equals("serviceName") && null != binaryAnnotation.value) {
          String serviceName = new String(binaryAnnotation.value, Charset.forName("UTF-8"));
          if (serviceName.length() > 0) {
            return serviceName;
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
      if (!StringUtils.isEmpty(serviceName) && !serviceName.startsWith(serviceGroup)) {
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
  }

  private Set<String> tagSet = Sets.newHashSet();

  private void storeTreeTag(String treeTag, String nodePath) throws ExecutionException, InterruptedException {
    String existTag = treeTag;
    if (!tagSet.contains(existTag))  {
      BoundStatement selectBound = CassandraUtil.bindWithName(selectServiceTree, "select-tree-tag").setString("tag",treeTag);
      existTag = transform(session.executeAsync(selectBound), new Function<ResultSet, String>() {
        @Nullable
        @Override
        public String apply(@Nullable ResultSet input) {
          if (null != input) {
            Row row = input.one();
            if (null != row) return row.getString("tag");
          }
          return null;
        }
      }).get();
      tagSet.add(existTag);
    }

    if (StringUtils.isEmpty(existTag)) {
      BoundStatement bound = CassandraUtil.bindWithName(insertServiceTree, "insert-tree-tag")
        .setString("tag", treeTag)
        .setSet("nodePath", Sets.newHashSet(nodePath));
      setTtl(bound);
      deduplicatingExecutor.maybeExecuteAsync(bound, nodePath);
    } else {
      BoundStatement bound = CassandraUtil.bindWithName(updateServiceTree, "update-tree-tag")
        .setString("tag", treeTag)
        .setSet("nodePath", Sets.newHashSet(nodePath));
      deduplicatingExecutor.maybeExecuteAsync(bound, nodePath);
    }
  }

  private void storeTreeNode(ZeusServiceTreeNode zeusServiceTreeNode) {
    BoundStatement bound = CassandraUtil.bindWithName(insertServiceTreeNode, "insert-tree-node")
      .setString("serviceName", zeusServiceTreeNode.getServiceName())
      .setString("parentService", zeusServiceTreeNode.getParentService())
      .setString("rootService", zeusServiceTreeNode.getRootService())
      .setString("tag", zeusServiceTreeNode.getTreeTag());
    setTtl(bound);
    deduplicatingExecutor.maybeExecuteAsync(bound, zeusServiceTreeNode.toString());
  }

  private void setTtl(BoundStatement bound) {
    bound.setInt("ttl_",ZEUS_TTL);
  }

  class ZeusServiceTreeNode {

    private String serviceName;

    private String parentService;

    private String rootService;

    private String treeTag;

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    public String getParentService() {
      return parentService;
    }

    public void setParentService(String parentService) {
      this.parentService = parentService;
    }

    public String getRootService() {
      return rootService;
    }

    public void setRootService(String rootService) {
      this.rootService = rootService;
    }

    public String getTreeTag() {
      return treeTag;
    }

    public void setTreeTag(String treeTag) {
      this.treeTag = treeTag;
    }

    @Override
    public String toString() {
      return "ZeusServiceTreeNode{" +
        "serviceName='" + serviceName + '\'' +
        ", parentService='" + parentService + '\'' +
        ", rootService='" + rootService + '\'' +
        ", treeTag='" + treeTag + '\'' +
        '}';
    }
  }

  class ZeusServiceTree {

    private String treeTag;

    private String treeJson;

    public String getTreeTag() {
      return treeTag;
    }

    public void setTreeTag(String treeTag) {
      this.treeTag = treeTag;
    }

    public String getTreeJson() {
      return treeJson;
    }

    public void setTreeJson(String treeJson) {
      this.treeJson = treeJson;
    }
  }
}
