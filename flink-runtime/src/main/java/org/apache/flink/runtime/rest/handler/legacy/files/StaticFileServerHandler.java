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

package org.apache.flink.runtime.rest.handler.legacy.files;

/**
 * *************************************************************************** This code is based on
 * the "HttpStaticFileServerHandler" from the Netty project's HTTP server example.
 *
 * <p>See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 * ***************************************************************************
 */
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.LeaderRetrievalHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.handler.util.MimeTypes;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;

import org.slf4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.DATE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.EXPIRES;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.IF_MODIFIED_SINCE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.LAST_MODIFIED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple file server handler that serves requests to web frontend's static files, such as HTML,
 * CSS, or JS files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.
 */
@ChannelHandler.Sharable
public class StaticFileServerHandler<T extends RestfulGateway> extends LeaderRetrievalHandler<T> {

    /** Timezone in which this server answers its "if-modified" requests. */
    private static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

    /** Date format for HTTP. */
    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";

    /** Be default, we allow files to be cached for 5 minutes. */
    private static final int HTTP_CACHE_SECONDS = 300;

    // ------------------------------------------------------------------------

    /** The path in which the static documents are. */
    private final File rootPath;

    public StaticFileServerHandler(
            GatewayRetriever<? extends T> retriever, Duration timeout, File rootPath)
            throws IOException {

        super(retriever, timeout, Collections.emptyMap());

        this.rootPath = checkNotNull(rootPath).getCanonicalFile();
    }

    // ------------------------------------------------------------------------
    //  Responses to requests
    // ------------------------------------------------------------------------

    @Override
    protected void respondAsLeader(
            ChannelHandlerContext channelHandlerContext, RoutedRequest routedRequest, T gateway)
            throws Exception {
        final HttpRequest request = routedRequest.getRequest();
        final String requestPath;

        // make sure we request the "index.html" in case there is a directory request
        if (routedRequest.getPath().endsWith("/")) {
            requestPath = routedRequest.getPath() + "index.html";
        } else {
            requestPath = routedRequest.getPath();
        }

        try {
            respondToRequest(channelHandlerContext, request, requestPath);
        } catch (RestHandlerException rhe) {
            HandlerUtils.sendErrorResponse(
                    channelHandlerContext,
                    routedRequest.getRequest(),
                    new ErrorResponseBody(rhe.getMessage()),
                    rhe.getHttpResponseStatus(),
                    responseHeaders);
        }
    }

    /** Response when running with leading JobManager. */
    private void respondToRequest(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath)
            throws IOException, ParseException, URISyntaxException, RestHandlerException {

        // convert to absolute path
        final File file = new File(rootPath, requestPath);

        if (!file.exists()) {
            // file does not exist. Try to load it with the classloader
            ClassLoader cl = StaticFileServerHandler.class.getClassLoader();

            try (InputStream resourceStream = cl.getResourceAsStream("web" + requestPath)) {
                boolean success = false;
                try {
                    if (resourceStream != null) {
                        URL root = cl.getResource("web");
                        URL requested = cl.getResource("web" + requestPath);

                        if (root != null && requested != null) {
                            URI rootURI = new URI(root.getPath()).normalize();
                            URI requestedURI = new URI(requested.getPath()).normalize();

                            // Check that we don't load anything from outside of the
                            // expected scope.
                            if (!rootURI.relativize(requestedURI).equals(requestedURI)) {
                                logger.debug(
                                        "Loading missing file from classloader: {}", requestPath);
                                // ensure that directory to file exists.
                                file.getParentFile().mkdirs();
                                Files.copy(resourceStream, file.toPath());

                                success = true;
                            }
                        }
                    }
                } catch (Throwable t) {
                    logger.error("error while responding", t);
                } finally {
                    if (!success) {
                        logger.debug(
                                "Unable to load requested file {} from classloader", requestPath);
                        throw new NotFoundException(
                                String.format("Unable to load requested file %s.", requestPath));
                    }
                }
            }
        }

        checkFileValidity(file, rootPath, logger);

        // cache validation
        final String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
            Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            long fileLastModifiedSeconds = file.lastModified() / 1000;
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Responding 'NOT MODIFIED' for file '" + file.getAbsolutePath() + '\'');
                }

                sendNotModified(ctx);
                return;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Responding with file '" + file.getAbsolutePath() + '\'');
        }

        // Don't need to close this manually. Netty's DefaultFileRegion will take care of it.
        final RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not find file {}.", file.getAbsolutePath());
            }
            throw new NotFoundException("File not found.");
        }

        try {
            long fileLength = raf.length();

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            setContentTypeHeader(response, file);
            setDateAndCacheHeaders(response, file);

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            HttpUtil.setContentLength(response, fileLength);

            // write the initial line and the header.
            ctx.write(response);

            // write the content.
            ChannelFuture lastContentFuture;
            if (ctx.pipeline().get(SslHandler.class) == null) {
                ctx.write(
                        new DefaultFileRegion(raf.getChannel(), 0, fileLength),
                        ctx.newProgressivePromise());
                lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            } else {
                lastContentFuture =
                        ctx.writeAndFlush(
                                new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)),
                                ctx.newProgressivePromise());
                // HttpChunkedInput will write the end marker (LastHttpContent) for us.
            }

            // close the connection, if no keep-alive is needed
            if (!HttpUtil.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Exception e) {
            raf.close();
            logger.error("Failed to serve file.", e);
            throw new RestHandlerException("Internal server error.", INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (ctx.channel().isActive()) {
            logger.error("Caught exception", cause);
            HandlerUtils.sendErrorResponse(
                    ctx,
                    false,
                    new ErrorResponseBody("Internal server error."),
                    INTERNAL_SERVER_ERROR,
                    Collections.emptyMap());
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities to encode headers and responses
    // ------------------------------------------------------------------------

    /**
     * Send the "304 Not Modified" response. This response can be used when the file timestamp is
     * the same as what the browser is sending up.
     *
     * @param ctx The channel context to write the response to.
     */
    public static void sendNotModified(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED);
        setDateHeader(response);

        // close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Sets the "date" header for the HTTP response.
     *
     * @param response HTTP response
     */
    public static void setDateHeader(FullHttpResponse response) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(GMT_TIMEZONE);

        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));
    }

    /**
     * Sets the "date" and "cache" headers for the HTTP Response.
     *
     * @param response The HTTP response object.
     * @param fileToCache File to extract the modification timestamp from.
     */
    public static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(GMT_TIMEZONE);

        // date header
        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));

        // cache headers
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers()
                .set(LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
    }

    /**
     * Sets the content type header for the HTTP Response.
     *
     * @param response HTTP response
     * @param file file to extract content type
     */
    public static void setContentTypeHeader(HttpResponse response, File file) {
        String mimeType = MimeTypes.getMimeTypeForFileName(file.getName());
        String mimeFinal = mimeType != null ? mimeType : MimeTypes.getDefaultMimeType();
        response.headers().set(CONTENT_TYPE, mimeFinal);
    }

    /**
     * Checks various conditions for file access. If all checks pass this method returns, and
     * processing of the request may continue. If any check fails this method throws a {@link
     * RestHandlerException}, and further processing of the request must be limited to sending an
     * error response.
     */
    public static void checkFileValidity(File file, File rootPath, Logger logger)
            throws IOException, RestHandlerException {
        // this check must be done first to prevent probing for arbitrary files
        if (!file.getCanonicalFile().toPath().startsWith(rootPath.toPath())) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Requested path {} points outside the root directory.",
                        file.getAbsolutePath());
            }
            throw new RestHandlerException("Forbidden.", FORBIDDEN);
        }

        if (!file.exists() || file.isHidden()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Requested path {} cannot be found.", file.getAbsolutePath());
            }
            throw new RestHandlerException("File not found.", NOT_FOUND);
        }

        if (file.isDirectory() || !file.isFile()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Requested path {} does not point to a file.", file.getAbsolutePath());
            }
            throw new RestHandlerException("File not found.", METHOD_NOT_ALLOWED);
        }
    }
}
