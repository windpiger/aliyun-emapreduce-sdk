/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fs.oss.utils;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class ResourceLoader {
  private static final Log LOG = LogFactory.getLog(ResourceLoader.class);
  private URLClassLoader urlClassLoader = null;

  private ResourceLoader() {
  }

  public static ResourceLoader getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static List<URL> geClassLoaderURLs(Configuration conf)
      throws Exception {
    String dependPath = conf.get("fs.oss.core.dependency.path");
    String[] sdkDeps = null;
    Boolean runLocal = conf.getBoolean("mapreduce.job.run-local", false);
    if ((dependPath == null || dependPath.isEmpty()) && !runLocal) {
      throw new RuntimeException("Job dose not run locally, set " +
          "\"fs.oss.core.dependency.path\" first please.");
    } else if (dependPath == null || dependPath.isEmpty()) {
      LOG.info("\"mapreduce.job.run-local\" set true.");
    } else {
      sdkDeps = dependPath.split(",");
    }

    ArrayList<URL> urls = new ArrayList<URL>();
    if (sdkDeps != null) {
      for (String dep : sdkDeps) {
        urls.add(new URL("file://" + dep));
      }
    }
    String[] cp;
    if (conf.getBoolean("mapreduce.job.run-local", false)) {
      if (SystemUtils.IS_OS_WINDOWS) {
        cp = System.getProperty("java.class.path").split(";");

      } else {
        cp = System.getProperty("java.class.path").split(":");
      }
      for (String entity : cp) {
        urls.add(new URL("file:" + entity));
      }
    }

    return urls;
  }

  @SuppressWarnings("unchecked")
  public synchronized URLClassLoader getUrlClassLoader(Configuration conf) {
    if (urlClassLoader == null) {
      try {
        List<URL> urls = geClassLoaderURLs(conf);
        urlClassLoader = new URLClassLoader(urls.toArray(new URL[0]), null);
      } catch (Exception e) {
        throw new RuntimeException("Can not initialize OSS URLClassLoader, " +
            e.getMessage());
      }
    }
    return urlClassLoader;
  }

  private static class LazyHolder {
    private static final ResourceLoader INSTANCE = new ResourceLoader();
  }
}
