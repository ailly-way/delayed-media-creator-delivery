FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go build -o /app ./...
FROM alpine
COPY --from=build /app /app
ENV INFRAI_API_KEY=""
ENTRYPOINT ["/app"]
