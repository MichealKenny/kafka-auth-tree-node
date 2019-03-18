<!--
 * The contents of this file are subject to the terms of the Common Development and
 * Distribution License (the License). You may not use this file except in compliance with the
 * License.
 *
 * You can obtain a copy of the License at legal/CDDLv1.0.txt. See the License for the
 * specific language governing permission and limitations under the License.
 *
 * When distributing Covered Software, include this CDDL Header Notice in each file and include
 * the License file at legal/CDDLv1.0.txt. If applicable, add the following below the CDDL
 * Header, with the fields enclosed by brackets [] replaced by your own identifying
 * information: "Portions copyright [year] [name of copyright owner]".
 *
 * Copyright 2019 ForgeRock.
-->
<b>Kafka Authentication Node</b>
<br/>
A simple authentication node for the [ForgeRock Identity Platform][forgerock_platform]. This node allows you to record authentication events to a Kafka topic.

![ScreenShot](./kafka-node.png) 

## Installation

Copy the kafka-auth-tree-node-6.5.0-jar-with-dependencies.jar file from the ../target directory into the ../web-container/webapps/openam/WEB-INF/lib directory where AM is deployed.  Restart the web container to pick up the new node.  The node will then appear in the authentication trees components palette.
## Usage

The node requires you provide it a Kafka broker and topic, and a configurable event to record (such as a failed or successful login)

## To Build
The code in this repository has binary dependencies that live in the ForgeRock maven repository. Maven can be configured to authenticate to this repository by following the following [ForgeRock Knowledge Base Article](https://backstage.forgerock.com/knowledge/kb/article/a74096897).

Edit the necessary KafkaProducerNode.java as appropriate.  To rebuild, run "mvn clean install" in the directory containing the pom.xml   


## To Do

* Producer: Add option to output as JSON.
* Producer: Allow any shared state variable to be sent to the Kafka topic.
* Producer: Enable Mutual TLS authentication to the Kafka broker.
* Consumer: Investigate implementation methods for a Kafka Consumer Node.


## Disclaimer
The sample code described herein is provided on an "as is" basis, without warranty of any kind, to the fullest extent permitted by law. ForgeRock does not warrant or guarantee the individual success developers may have in implementing the sample code on their development platforms or in production configurations.

ForgeRock does not warrant, guarantee or make any representations regarding the use, results of use, accuracy, timeliness or completeness of any data or information relating to the sample code. ForgeRock disclaims all warranties, expressed or implied, and in particular, disclaims all warranties of merchantability, and warranties related to the code, or any service or software related thereto.

ForgeRock shall not be liable for any direct, indirect or consequential damages or costs of any type arising out of any action taken by you or others related to the sample code.

[forgerock_platform]: https://www.forgerock.com/platform/