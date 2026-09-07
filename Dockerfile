FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rescueflow ./cmd/rescueflow

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rescueflow /rescueflow
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/rescueflow"]
