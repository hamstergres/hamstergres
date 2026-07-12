FROM golang:1.24-alpine AS build
WORKDIR /src
RUN apk add --no-cache build-base
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/hamstergres-proxy ./cmd/hamstergres-proxy

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/hamstergres-proxy /usr/local/bin/hamstergres-proxy
ENTRYPOINT ["hamstergres-proxy"]
