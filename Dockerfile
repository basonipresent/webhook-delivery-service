FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# Package containing main. Set via MAIN_PKG in the Makefile / compose.yaml.
ARG MAIN_PKG=.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ${MAIN_PKG}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENV PORT=8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app"]