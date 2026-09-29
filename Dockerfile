FROM golang:1.26 AS build
WORKDIR /build
ENV GOVCS=*:off
ENV GOFLAGS=-buildvcs=false
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal
COPY testdata ./testdata
RUN go test ./... && go vet ./...
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /collector .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /collector /collector
ENTRYPOINT ["/collector"]
