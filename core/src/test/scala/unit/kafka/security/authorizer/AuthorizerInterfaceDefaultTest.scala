/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.authorizer

import java.util.concurrent.CompletionStage
import java.{lang, util}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.server.QuorumTestHarness
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl._
import org.apache.kafka.controller.QuorumController
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.server.authorizer._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.util.Properties

/**
 * Tests the default method implementations in Authorizer. We do that by ensuring they are not
 * overridden in this test
 */
class AuthorizerInterfaceDefaultTest extends QuorumTestHarness with BaseAuthorizerTest {

  private var interfaceDefaultAuthorizer: Authorizer = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    interfaceDefaultAuthorizer = new DelegateAuthorizer(createUnderlyingAuthorizer)
  }

  override def authorizer: Authorizer = interfaceDefaultAuthorizer

  override protected def kraftControllerConfigs(): Seq[Properties] = {
    val props = new Properties
    props.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[StandardAuthorizer].getName)
    props.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, superUsers)
    Seq(props)
  }

  def createUnderlyingAuthorizer: Authorizer = {
    if (isKRaftTest()) {
      TestUtils.waitUntilTrue(() => !controllerServer.controller.asInstanceOf[QuorumController].needToCompleteAuthorizerLoad, "fooo")
      controllerServer.authorizer.get
    } else {
      val aclAuthorizer = new AclAuthorizer

      // Increase maxUpdateRetries to avoid transient failures
      aclAuthorizer.maxUpdateRetries = Int.MaxValue

      val props = TestUtils.createBrokerConfig(0, zkConnect)
      props.put(AclAuthorizer.SuperUsersProp, superUsers)
      val config = KafkaConfig.fromProps(props)
      aclAuthorizer.configure(config.originals)

      aclAuthorizer
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    interfaceDefaultAuthorizer.close()
    super.tearDown()
  }

  /*
   * We intentionally do not override the methods that have a default implementation in Authorizer.
   */
  class DelegateAuthorizer(authorizer: Authorizer) extends Authorizer {

    override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
      authorizer.start(serverInfo)
    }

    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      authorizer.authorize(requestContext, actions)
    }

    override def createAcls(requestContext: AuthorizableRequestContext, aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
      authorizer.createAcls(requestContext, aclBindings)
    }

    override def deleteAcls(requestContext: AuthorizableRequestContext, aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
      authorizer.deleteAcls(requestContext, aclBindingFilters)
    }

    override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = {
      authorizer.acls(filter)
    }

    override def configure(configs: util.Map[String, _]): Unit = {
      authorizer.configure(configs)
    }

    override def close(): Unit = {
      authorizer.close()
    }
  }

}
