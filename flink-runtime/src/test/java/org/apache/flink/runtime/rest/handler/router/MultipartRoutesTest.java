package org.apache.flink.runtime.rest.handler.router;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipartRoutesTest {

    @Test
    public void testRoutePatternMatching() {
        MultipartRoutes.Builder builder = new MultipartRoutes.Builder();
        builder.addPostRoute("/jobs");
        builder.addPostRoute("/jobs/:jobid/stop");
        builder.addPostRoute("/jobs/:jobid/savepoints/:savepointid/delete");

        builder.addFileUploadRoute("/jobs");
        builder.addFileUploadRoute("/jobs/:jobid/stop");
        builder.addFileUploadRoute("/jobs/:jobid/savepoints/:savepointid/delete");

        MultipartRoutes routes = builder.build();

        assertThat(routes.isPostRoute("/jobs")).isTrue();
        assertThat(routes.isPostRoute("/jobs?q1=p1&q2=p2")).isTrue();
        assertThat(routes.isPostRoute("/jobs/abc")).isFalse();
        assertThat(routes.isPostRoute("/jobs/abc/stop")).isTrue();
        assertThat(routes.isPostRoute("/jobs/abc/savepoints/def/delete")).isTrue();

        assertThat(routes.isFileUploadRoute("/jobs")).isTrue();
        assertThat(routes.isFileUploadRoute("/jobs?q1=p1&q2=p2")).isTrue();
        assertThat(routes.isFileUploadRoute("/jobs/abc")).isFalse();
        assertThat(routes.isFileUploadRoute("/jobs/abc/stop")).isTrue();
        assertThat(routes.isFileUploadRoute("/jobs/abc/savepoints/def/delete")).isTrue();
    }
}
