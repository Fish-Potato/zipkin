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

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author onefish
 * @date 2017/11/8 0008.
 */
public class SampleUtil {

  private static final LoadingCache<String, Boolean> cache;

  static {
    cache = CacheBuilder.newBuilder()
      .expireAfterWrite(60, TimeUnit.SECONDS)
      .ticker(new Ticker() {
        @Override public long read() {
          return System.nanoTime();
        }
      })
      .maximumSize(10000L)
      .build(new CacheLoader<String, Boolean>() {

        @Override
        public Boolean load(String key) throws Exception {
          return true;
        }
      });
  }



  public static boolean ifSample(String key) {
    try {
      Boolean needSample = cache.get(key);
      if (needSample) {
        cache.put(key,false);
        return true;
      }
    } catch (ExecutionException e) {

    }
    return false;
  }
}
