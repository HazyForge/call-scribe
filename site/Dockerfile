# syntax=docker/dockerfile:1.7
# Build from the call-scribe repository root:
#   docker build -f site/Dockerfile -t ghcr.io/hazyforge/call-scribe-site:dev .

FROM node:22-alpine AS build
WORKDIR /src
RUN corepack enable && corepack prepare pnpm@10.25.0 --activate

COPY site /src/site
WORKDIR /src/site
RUN pnpm install --frozen-lockfile
RUN pnpm build

FROM nginxinc/nginx-unprivileged:1.27-alpine AS runtime
COPY site/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /src/site/dist /usr/share/nginx/html
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
