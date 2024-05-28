package org.apache.flink.runtime.rest.handler.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility for {@link org.apache.flink.runtime.rest.FileUploadHandler} to determine whether it
 * should accept a request.
 */
public class MultipartRoutes {

    private final List<PathPattern> postRoutes;
    private final List<PathPattern> fileUploadRoutes;

    private MultipartRoutes(List<PathPattern> postRoutes, List<PathPattern> fileUploadRoutes) {
        this.postRoutes = new ArrayList<>(postRoutes);
        this.fileUploadRoutes = new ArrayList<>(fileUploadRoutes);
    }

    /**
     * Returns <code>true</code> if the handler at the provided <code>requestUri</code> endpoint
     * accepts POST requests.
     *
     * @param requestUri URI for the request
     */
    public boolean isPostRoute(String requestUri) {
        return checkRoutes(requestUri, postRoutes);
    }

    /**
     * Returns <code>true</code> if the handler at the provided <code>requestUri</code> endpoint
     * accepts file uploads.
     *
     * @param requestUri URI for the request
     */
    public boolean isFileUploadRoute(String requestUri) {
        return checkRoutes(requestUri, fileUploadRoutes);
    }

    @Override
    public String toString() {
        return "MultipartRoutes{"
                + "postRoutes="
                + postRoutes
                + ", fileUploadRoutes="
                + fileUploadRoutes
                + '}';
    }

    private boolean checkRoutes(String requestUri, List<PathPattern> routes) {
        String[] pathTokens = Router.decodePathTokens(requestUri);
        Map<String, String> params = new HashMap<>();
        for (PathPattern route : routes) {
            if (route.match(pathTokens, params)) {
                return true;
            }
        }
        return false;
    }

    public static class Builder {

        private final List<PathPattern> postRoutes = new ArrayList<>();
        private final List<PathPattern> fileUploadRoutes = new ArrayList<>();

        public Builder() {}

        public MultipartRoutes.Builder addPostRoute(String pattern) {
            postRoutes.add(new PathPattern(pattern));
            return this;
        }

        public MultipartRoutes.Builder addFileUploadRoute(String pattern) {
            fileUploadRoutes.add(new PathPattern(pattern));
            return this;
        }

        public MultipartRoutes build() {
            return new MultipartRoutes(postRoutes, fileUploadRoutes);
        }
    }
}
