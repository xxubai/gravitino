/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.flink.connector.hive;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestGravitinoHiveCatalog {

  @Test
  void testInjectedHiveCatalogIsUsed() {
    HiveCatalog hiveCatalog = mock(HiveCatalog.class);

    TestCatalog catalog = new TestCatalog(hiveCatalog);

    Assertions.assertSame(hiveCatalog, catalog.nativeCatalog());
  }

  @Test
  void testApplyS3SecretKeyCredential() {
    Configuration configuration = applyCredential(new S3SecretKeyCredential("access", "secret"));

    Assertions.assertEquals("access", configuration.get("fs.s3a.access.key"));
    Assertions.assertEquals("secret", configuration.get("fs.s3a.secret.key"));
    Assertions.assertNull(configuration.get("fs.s3a.session.token"));
    Assertions.assertNull(configuration.get("fs.s3a.aws.credentials.provider"));
  }

  @Test
  void testApplyS3TokenCredential() {
    Configuration configuration =
        applyCredential(new S3TokenCredential("access", "secret", "token", 1L));

    assertTemporaryS3Credential(configuration);
  }

  @Test
  void testApplyAwsIrsaCredential() {
    Configuration configuration =
        applyCredential(new AwsIrsaCredential("access", "secret", "token", 1L));

    assertTemporaryS3Credential(configuration);
  }

  @Test
  void testExistingCredentialTypesRemainSupported() {
    Configuration ossConfiguration =
        applyCredential(new OSSSecretKeyCredential("oss-access", "oss-secret"));
    Assertions.assertEquals("oss-access", ossConfiguration.get("fs.oss.accessKeyId"));
    Assertions.assertEquals("oss-secret", ossConfiguration.get("fs.oss.accessKeySecret"));

    Configuration azureConfiguration =
        applyCredential(new AzureAccountKeyCredential("account", "account-key"));
    Assertions.assertEquals(
        "account-key", azureConfiguration.get("fs.azure.account.key.account.dfs.core.windows.net"));

    Assertions.assertDoesNotThrow(() -> applyCredential(mock(Credential.class)));
  }

  private static Configuration applyCredential(Credential credential) {
    Catalog catalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(catalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {credential});
    Configuration configuration = new Configuration(false);

    GravitinoHiveCatalog.applyS3Credential(catalog, configuration);

    return configuration;
  }

  private static void assertTemporaryS3Credential(Configuration configuration) {
    Assertions.assertEquals("access", configuration.get("fs.s3a.access.key"));
    Assertions.assertEquals("secret", configuration.get("fs.s3a.secret.key"));
    Assertions.assertEquals("token", configuration.get("fs.s3a.session.token"));
    Assertions.assertEquals(
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        configuration.get("fs.s3a.aws.credentials.provider"));
  }

  private static class TestCatalog extends GravitinoHiveCatalog {

    private TestCatalog(HiveCatalog hiveCatalog) {
      super(
          "hive",
          "default",
          Collections.emptyMap(),
          mock(SchemaAndTablePropertiesConverter.class),
          mock(PartitionConverter.class),
          hiveCatalog);
    }

    private AbstractCatalog nativeCatalog() {
      return realCatalog();
    }
  }
}
