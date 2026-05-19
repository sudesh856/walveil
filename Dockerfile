FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o walveil ./cmd/walveil


FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /src/walveil /walveil

USER 65532:65532

ENTRYPOINT [ "/walveil" ]