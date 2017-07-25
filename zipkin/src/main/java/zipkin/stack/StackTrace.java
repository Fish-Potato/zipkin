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
package zipkin.stack;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by onefish on 2017/6/19 0019.
 */
public class StackTrace {

    /**
     * 存储每一次调用
     */
    private List<TraceEntry> entryList = new ArrayList<>();

    /**
     * zipkin的traceId
     */
    private String traceId;

    /**
     * zipkin当前span的id
     */
    private String spanId;
    /**
     * 当前线程的描述信息
     */
    private String serviceName;

    /**
     * 线程开始的时间
     */
    private long beginTime;

    /**
     * 线程结束的时间
     */
    private long endTime;

    /**
     * 存储异常信息
     */
    private Set<TraceException> traceExceptionSet;

    public List<TraceEntry> getEntryList() {
        return entryList;
    }

    public void setEntryList(List<TraceEntry> entryList) {
        this.entryList = entryList;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Set<TraceException> getTraceExceptionSet() {
        return traceExceptionSet;
    }

    public void setTraceExceptionSet(Set<TraceException> traceExceptionSet) {
        this.traceExceptionSet = traceExceptionSet;
    }


}
