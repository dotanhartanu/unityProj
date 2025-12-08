# Frontend - Nginx web server
# Uses Alpine base image for minimal size and attack surface

FROM nginx:alpine

# Copy custom nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Copy static files (HTML, CSS, JS)
COPY static/ /usr/share/nginx/html/

# Set ownership for non-root nginx user
# nginx:alpine runs as user 'nginx' (UID 101)
RUN chown -R nginx:nginx /usr/share/nginx/html && \
    chmod -R 755 /usr/share/nginx/html

# Port for HTTP traffic (documentation only)
EXPOSE 80

# Health check for Docker (Kubernetes uses own probes)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost/health || exit 1

# CMD inherited from base image: ["nginx", "-g", "daemon off;"]
