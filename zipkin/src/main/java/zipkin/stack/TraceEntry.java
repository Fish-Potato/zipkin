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

/**
 * Created by onefish on 2017/6/19 0019.
 */
public class TraceEntry {

  private String className;
  private String method;
  private long enterTimestamp;
  private String event;
  private String level ;
  private long exitTimestamp;
  private Object params;

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public long getEnterTimestamp() {
    return enterTimestamp;
  }

  public void setEnterTimestamp(long enterTimestamp) {
    this.enterTimestamp = enterTimestamp;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public long getExitTimestamp() {
    return exitTimestamp;
  }

  public void setExitTimestamp(long exitTimestamp) {
    this.exitTimestamp = exitTimestamp;
  }

  public Object getParams() {
    return params;
  }

  public void setParams(Object params) {
    this.params = params;
  }

}
