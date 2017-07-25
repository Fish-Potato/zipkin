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
package zipkin.server;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
import zipkin.*;
import zipkin.stack.JsonUtils;
import zipkin.stack.StackTrace;
import zipkin.stack.TraceEntry;
import zipkin.stack.TraceException;
import zipkin.storage.QueryRequest;
import zipkin.storage.StorageComponent;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static zipkin.internal.Util.UTF_8;
import static zipkin.internal.Util.lowerHexToUnsignedLong;

/**
 * Implements the json api used by the Zipkin UI
 *
 * See com.twitter.zipkin.query.ZipkinQueryController
 */
@RestController
@RequestMapping("/api/v1")
@CrossOrigin("${zipkin.query.allowed-origins:*}")
@ConditionalOnProperty(name = "zipkin.query.enabled", matchIfMissing = true)
public class ZipkinQueryApiV1 {

  @Autowired
  @Value("${zipkin.query.lookback:86400000}")
  int defaultLookback = 86400000; // 1 day in millis

  /** The Cache-Control max-age (seconds) for /api/v1/services and /api/v1/spans */
  @Value("${zipkin.query.names-max-age:300}")
  int namesMaxAge = 300; // 5 minutes
  volatile int serviceCount; // used as a threshold to start returning cache-control headers

  private final StorageComponent storage;

  private ThreadLocal<Map<String,Long>> idMapLocal = new ThreadLocal<>();

  private ThreadLocal<Long> autoIdLocal = new ThreadLocal<>();

  @Autowired
  public ZipkinQueryApiV1(StorageComponent storage) {
    this.storage = storage; // don't cache spanStore here as it can cause the app to crash!
  }

  @RequestMapping(value = "/dependencies", method = RequestMethod.GET, produces = APPLICATION_JSON_VALUE)
  public byte[] getDependencies(@RequestParam(value = "endTs", required = true) long endTs,
                                @RequestParam(value = "lookback", required = false) Long lookback) {
    return Codec.JSON.writeDependencyLinks(storage.spanStore().getDependencies(endTs, lookback != null ? lookback : defaultLookback));
  }

  @RequestMapping(value = "/services", method = RequestMethod.GET)
  public ResponseEntity<List<String>> getServiceNames() {
    List<String> serviceNames = storage.spanStore().getServiceNames();
    serviceCount = serviceNames.size();
    return maybeCacheNames(serviceNames);
  }

  @RequestMapping(value = "/spans", method = RequestMethod.GET)
  public ResponseEntity<List<String>> getSpanNames(
      @RequestParam(value = "serviceName", required = true) String serviceName) {
    return maybeCacheNames(storage.spanStore().getSpanNames(serviceName));
  }

  @RequestMapping(value = "/traces", method = RequestMethod.GET, produces = APPLICATION_JSON_VALUE)
  public String getTraces(
      @RequestParam(value = "serviceName", required = false) String serviceName,
      @RequestParam(value = "spanName", defaultValue = "all") String spanName,
      @RequestParam(value = "annotationQuery", required = false) String annotationQuery,
      @RequestParam(value = "minDuration", required = false) Long minDuration,
      @RequestParam(value = "maxDuration", required = false) Long maxDuration,
      @RequestParam(value = "endTs", required = false) Long endTs,
      @RequestParam(value = "lookback", required = false) Long lookback,
      @RequestParam(value = "limit", required = false) Integer limit) {
    QueryRequest queryRequest = QueryRequest.builder()
        .serviceName(serviceName)
        .spanName(spanName)
        .parseAnnotationQuery(annotationQuery)
        .minDuration(minDuration)
        .maxDuration(maxDuration)
        .endTs(endTs)
        .lookback(lookback != null ? lookback : defaultLookback)
        .limit(limit).build();

    return new String(Codec.JSON.writeTraces(storage.spanStore().getTraces(queryRequest)), UTF_8);
  }

  @RequestMapping(value = "/trace/{traceIdHex}", method = RequestMethod.GET, produces = APPLICATION_JSON_VALUE)
  public String getTrace(@PathVariable String traceIdHex, WebRequest request) {
    long traceIdHigh = traceIdHex.length() == 32 ? lowerHexToUnsignedLong(traceIdHex, 0) : 0L;
    long traceIdLow = lowerHexToUnsignedLong(traceIdHex);
    String[] raw = request.getParameterValues("raw"); // RequestParam doesn't work for param w/o value
    List<Span> trace = raw != null
        ? storage.spanStore().getRawTrace(traceIdHigh, traceIdLow)
        : storage.spanStore().getTrace(traceIdHigh, traceIdLow);
    if (trace == null) {
      throw new TraceNotFoundException(traceIdHex, traceIdHigh, traceIdLow);
    }
    String result = new String(Codec.JSON.writeSpans(addStackTraceSpan(trace)), UTF_8);

    return parseString(result);
  }


  @ExceptionHandler(TraceNotFoundException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public void notFound() {
  }

  static class TraceNotFoundException extends RuntimeException {
    public TraceNotFoundException(String traceIdHex, Long traceIdHigh, long traceId) {
      super(String.format("Cannot find trace for id=%s,  parsed value=%s", traceIdHex,
          traceIdHigh != null ? traceIdHigh + "," + traceId : traceId));
    }
  }

  /**
   * We cache names if there are more than 3 services. This helps people getting started: if we
   * cache empty results, users have more questions. We assume caching becomes a concern when zipkin
   * is in active use, and active use usually implies more than 3 services.
   */
  ResponseEntity<List<String>> maybeCacheNames(List<String> names) {
    ResponseEntity.BodyBuilder response = ResponseEntity.ok();
    if (serviceCount > 3) {
      response.cacheControl(CacheControl.maxAge(namesMaxAge, TimeUnit.SECONDS).mustRevalidate());
    }
    return response.body(names);
  }

  private List<Span> addStackTraceSpan(List<Span> trace) {
    List<Span> withStackTraceSpan = new ArrayList<>();
    List<Span> traceCopy = Lists.newArrayList(trace);
    // 0x100000000
    long stackSpanId = 4294967296L;
    for (Span span : trace) {
      for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
        if (binaryAnnotation.key.equals("stackTrace")) {
          StackTrace stackTrace = JSON.parseObject(new String(binaryAnnotation.value, Charset.forName("UTF-8")), StackTrace.class);
          for (TraceEntry traceEntry : stackTrace.getEntryList()) {
            String level = traceEntry.getLevel();
            long enterTime = traceEntry.getEnterTimestamp();
            long exitTime = traceEntry.getExitTimestamp();
            TraceException traceException = null;
            if (!CollectionUtils.isEmpty(stackTrace.getTraceExceptionSet())) {
              for (TraceException traceE : stackTrace.getTraceExceptionSet()) {
                if (isCurrentException(traceE, traceEntry)) {
                  traceException = traceE;
                }
              }
            }
            Span.Builder builder = Span.builder();

            builder.id(stackSpanId + getStackSpanId(traceEntry.getLevel()));
            if (null != getParentStackSpanId(level)) {
              builder.parentId(stackSpanId + getParentStackSpanId(level));
            } else {
              builder.parentId(span.id);
            }
            builder.duration(exitTime - enterTime);
            builder.name(buildSpanName(traceEntry));
            builder.timestamp(enterTime);
            builder.traceId(span.traceId);
            builder.traceIdHigh(span.traceIdHigh);

            Endpoint serviceEndpoint = binaryAnnotation.endpoint;
            Endpoint stackEndpoint = Endpoint.create(traceEntry.getEvent(), serviceEndpoint.ipv4);

            builder.addAnnotation(Annotation.create(enterTime,
              traceEntry.getEvent(), stackEndpoint));

            builder.addBinaryAnnotation(BinaryAnnotation.create("sa", serviceEndpoint.serviceName, stackEndpoint));

            builder.addBinaryAnnotation(BinaryAnnotation.create(
              "class", traceEntry.getClassName(), stackEndpoint));
            builder.addBinaryAnnotation(BinaryAnnotation.create(
              "method", traceEntry.getMethod(), stackEndpoint));
            if (null != traceEntry.getParams()) {
              builder.addBinaryAnnotation(BinaryAnnotation.create(
                "param", JsonUtils.obj2json(traceEntry.getParams()), stackEndpoint));
            }

            builder.addBinaryAnnotation(BinaryAnnotation.create(
              "event", traceEntry.getEvent(), stackEndpoint));
            if (null != traceException) {
              builder.addBinaryAnnotation(BinaryAnnotation.create(
                "exception", JSON.toJSONString(traceException),stackEndpoint));
              builder.addAnnotation(Annotation.create(enterTime,"error",stackEndpoint));
              builder.name(buildSpanName(traceEntry) + ": throw Exception!");
            }

            Span stackSpan = builder.build();
            withStackTraceSpan.add(stackSpan);
          }
          stackSpanId += 4294967296L;
        }
      }
    }
    if (!CollectionUtils.isEmpty(withStackTraceSpan)) {
      traceCopy.addAll(withStackTraceSpan);
    }
    return traceCopy;
  }

  private String buildSpanName(TraceEntry traceEntry) {
    String className = traceEntry.getClassName();
    if (className.contains(".")) {
      className = className.substring(className.lastIndexOf(".")+1);
    }
    return className + "." + traceEntry.getMethod();
  }

  private boolean isCurrentException(TraceException traceE, TraceEntry traceEntry) {
    if (traceE.getClassName().equals(traceEntry.getClassName())) {
      if (traceE.getMethodName().equals(traceEntry.getMethod())) {
        return true;
      }
      if (traceE.getMethodName().equals("afterCompletion") && traceEntry.getMethod().equals("preHandle")) {
        return true;
      }
    }
    return false;
  }


  /**
   * 通过映射表将 a.b.c.d的层级标识 转成long型id
   * @param level
   * @return
   */
  private Long getStackSpanId(String level) {
    if (null == idMapLocal.get()) {
      idMapLocal.set(new HashMap<>());
    }
    if (null == autoIdLocal.get()) {
      autoIdLocal.set(0L);
    }
    Long autoId = autoIdLocal.get();
    Map<String, Long> idMap = idMapLocal.get();
    if (idMap.containsKey(level)) {
      return idMap.get(level);
    } else {
      autoId++;
      idMap.put(level, autoId);
      autoIdLocal.set(autoId);
      return autoId;
    }
  }

  private Long getParentStackSpanId(String level) {
    if (!level.contains(".")) {
      return null;
    }
    return idMapLocal.get().get(level.substring(0,level.lastIndexOf(".")));
  }


  private String parseString (String result) {
    try {
      if (result.contains("stackTrace")) {
        JSONArray resultArray = JSON.parseArray(result);
        Object removeObject = null;
        for (Object o : resultArray) {
          JSONObject jsonObject = (JSONObject) o;

          for (Object bA : jsonObject.getJSONArray("binaryAnnotations")) {
            JSONObject binaryAnnotation = (JSONObject) bA;
            if (binaryAnnotation.containsKey("key")) {
              if (binaryAnnotation.getString("key").equals("stackTrace")) {
                removeObject = binaryAnnotation;
                break;
              }
            }
          }
          if (null != removeObject) {
            jsonObject.getJSONArray("binaryAnnotations").remove(removeObject);
          }
        }
        return resultArray.toJSONString();
      }
    } catch (Exception ignored) {
    }
    return result;
  }
}
