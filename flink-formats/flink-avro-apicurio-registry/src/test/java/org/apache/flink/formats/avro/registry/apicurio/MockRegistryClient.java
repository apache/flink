/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import io.apicurio.registry.rest.v2.beans.ArtifactOwner;
import io.apicurio.registry.rest.v2.beans.ArtifactReference;
import io.apicurio.registry.rest.v2.beans.ArtifactSearchResults;
import io.apicurio.registry.rest.v2.beans.ConfigurationProperty;
import io.apicurio.registry.rest.v2.beans.EditableMetaData;
import io.apicurio.registry.rest.v2.beans.GroupMetaData;
import io.apicurio.registry.rest.v2.beans.GroupSearchResults;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.rest.v2.beans.LogConfiguration;
import io.apicurio.registry.rest.v2.beans.NamedLogConfiguration;
import io.apicurio.registry.rest.v2.beans.RoleMapping;
import io.apicurio.registry.rest.v2.beans.Rule;
import io.apicurio.registry.rest.v2.beans.SortBy;
import io.apicurio.registry.rest.v2.beans.SortOrder;
import io.apicurio.registry.rest.v2.beans.UpdateState;
import io.apicurio.registry.rest.v2.beans.UserInfo;
import io.apicurio.registry.rest.v2.beans.VersionMetaData;
import io.apicurio.registry.rest.v2.beans.VersionSearchResults;
import io.apicurio.registry.types.RoleType;
import io.apicurio.registry.types.RuleType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/** Mock registry client. */
public class MockRegistryClient implements RegistryClient {
    private InputStream in = null;

    @Override
    public InputStream getLatestArtifact(String s, String s1) {
        return null;
    }

    @Override
    public ArtifactMetaData updateArtifact(
            String s,
            String s1,
            String s2,
            String s3,
            String s4,
            String s5,
            InputStream inputStream) {
        return null;
    }

    @Override
    public ArtifactMetaData updateArtifact(
            String s,
            String s1,
            String s2,
            String s3,
            String s4,
            InputStream inputStream,
            List<ArtifactReference> list) {
        return null;
    }

    @Override
    public void deleteArtifact(String s, String s1) {}

    @Override
    public ArtifactMetaData getArtifactMetaData(String s, String s1) {
        return null;
    }

    @Override
    public ArtifactOwner getArtifactOwner(String s, String s1) {
        return null;
    }

    @Override
    public void updateArtifactMetaData(String s, String s1, EditableMetaData editableMetaData) {}

    @Override
    public void updateArtifactOwner(String s, String s1, ArtifactOwner artifactOwner) {}

    @Override
    public VersionMetaData getArtifactVersionMetaDataByContent(
            String s, String s1, Boolean aBoolean, String s2, InputStream inputStream) {
        return null;
    }

    @Override
    public List<RuleType> listArtifactRules(String s, String s1) {
        return null;
    }

    @Override
    public void createArtifactRule(String s, String s1, Rule rule) {}

    @Override
    public void deleteArtifactRules(String s, String s1) {}

    @Override
    public Rule getArtifactRuleConfig(String s, String s1, RuleType ruleType) {
        return null;
    }

    @Override
    public Rule updateArtifactRuleConfig(String s, String s1, RuleType ruleType, Rule rule) {
        return null;
    }

    @Override
    public void deleteArtifactRule(String s, String s1, RuleType ruleType) {}

    @Override
    public void updateArtifactState(String s, String s1, UpdateState updateState) {}

    @Override
    public void testUpdateArtifact(String s, String s1, String s2, InputStream inputStream) {}

    @Override
    public InputStream getArtifactVersion(String s, String s1, String s2) {
        return null;
    }

    @Override
    public VersionMetaData getArtifactVersionMetaData(String s, String s1, String s2) {
        return null;
    }

    @Override
    public void updateArtifactVersionMetaData(
            String s, String s1, String s2, EditableMetaData editableMetaData) {}

    @Override
    public void deleteArtifactVersionMetaData(String s, String s1, String s2) {}

    @Override
    public void updateArtifactVersionState(
            String s, String s1, String s2, UpdateState updateState) {}

    @Override
    public VersionSearchResults listArtifactVersions(
            String s, String s1, Integer integer, Integer integer1) {
        return null;
    }

    @Override
    public VersionMetaData createArtifactVersion(
            String s,
            String s1,
            String s2,
            String s3,
            String s4,
            String s5,
            InputStream inputStream) {
        return null;
    }

    @Override
    public ArtifactSearchResults listArtifactsInGroup(
            String s, SortBy sortBy, SortOrder sortOrder, Integer integer, Integer integer1) {
        return null;
    }

    @Override
    public ArtifactMetaData createArtifact(
            String s,
            String s1,
            String s2,
            String s3,
            IfExists ifExists,
            Boolean aBoolean,
            String s4,
            String s5,
            String s6,
            String s7,
            String s8,
            InputStream inputStream) {
        return null;
    }

    @Override
    public ArtifactMetaData createArtifact(
            String s,
            String s1,
            String s2,
            String s3,
            IfExists ifExists,
            Boolean aBoolean,
            String s4,
            String s5,
            String s6,
            String s7,
            String s8,
            InputStream inputStream,
            List<ArtifactReference> list) {
        return null;
    }

    @Override
    public void deleteArtifactsInGroup(String s) {}

    @Override
    public void createArtifactGroup(GroupMetaData groupMetaData) {}

    @Override
    public void deleteArtifactGroup(String s) {}

    @Override
    public GroupMetaData getArtifactGroup(String s) {
        return null;
    }

    @Override
    public GroupSearchResults listGroups(
            SortBy sortBy, SortOrder sortOrder, Integer integer, Integer integer1) {
        return null;
    }

    @Override
    public InputStream getContentById(long l) {
        return null;
    }

    @Override
    public InputStream getContentByGlobalId(long l) {
        return null;
    }

    public void setInputStream(InputStream in) {
        this.in = in;
    }

    @Override
    public InputStream getContentByGlobalId(long l, Boolean aBoolean, Boolean aBoolean1) {
        return in;
    }

    @Override
    public InputStream getContentByHash(String s, Boolean aBoolean) {
        return null;
    }

    @Override
    public ArtifactSearchResults searchArtifacts(
            String s,
            String s1,
            String s2,
            List<String> list,
            List<String> list1,
            Long aLong,
            Long aLong1,
            SortBy sortBy,
            SortOrder sortOrder,
            Integer integer,
            Integer integer1) {
        return null;
    }

    @Override
    public ArtifactSearchResults searchArtifactsByContent(
            InputStream inputStream,
            SortBy sortBy,
            SortOrder sortOrder,
            Integer integer,
            Integer integer1) {
        return null;
    }

    @Override
    public List<RuleType> listGlobalRules() {
        return null;
    }

    @Override
    public void createGlobalRule(Rule rule) {}

    @Override
    public void deleteAllGlobalRules() {}

    @Override
    public Rule getGlobalRuleConfig(RuleType ruleType) {
        return null;
    }

    @Override
    public Rule updateGlobalRuleConfig(RuleType ruleType, Rule rule) {
        return null;
    }

    @Override
    public void deleteGlobalRule(RuleType ruleType) {}

    @Override
    public List<NamedLogConfiguration> listLogConfigurations() {
        return null;
    }

    @Override
    public NamedLogConfiguration getLogConfiguration(String s) {
        return null;
    }

    @Override
    public NamedLogConfiguration setLogConfiguration(String s, LogConfiguration logConfiguration) {
        return null;
    }

    @Override
    public NamedLogConfiguration removeLogConfiguration(String s) {
        return null;
    }

    @Override
    public InputStream exportData() {
        return null;
    }

    @Override
    public void importData(InputStream inputStream) {}

    @Override
    public void importData(InputStream inputStream, boolean b, boolean b1) {}

    @Override
    public List<RoleMapping> listRoleMappings() {
        return null;
    }

    @Override
    public void createRoleMapping(RoleMapping roleMapping) {}

    @Override
    public RoleMapping getRoleMapping(String s) {
        return null;
    }

    @Override
    public void updateRoleMapping(String s, RoleType roleType) {}

    @Override
    public void deleteRoleMapping(String s) {}

    @Override
    public UserInfo getCurrentUserInfo() {
        return null;
    }

    @Override
    public void setNextRequestHeaders(Map<String, String> map) {}

    @Override
    public Map<String, String> getHeaders() {
        return null;
    }

    @Override
    public List<ConfigurationProperty> listConfigProperties() {
        return null;
    }

    @Override
    public void setConfigProperty(String s, String s1) {}

    @Override
    public ConfigurationProperty getConfigProperty(String s) {
        return null;
    }

    @Override
    public void deleteConfigProperty(String s) {}

    @Override
    public List<ArtifactReference> getArtifactReferencesByGlobalId(long l) {
        return null;
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByContentId(long l) {
        return null;
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByContentHash(String s) {
        return null;
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByCoordinates(
            String s, String s1, String s2) {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
