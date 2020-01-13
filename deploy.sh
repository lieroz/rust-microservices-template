#!/bin/bash

docker pull "$BASE_IMAGE_NAME" || true
docker build --pull --cache-from "$BASE_IMAGE_NAME" --tag "$BASE_IMAGE_NAME" -f Dockerfile.build .
docker tag "$BASE_IMAGE_NAME" "${BASE_IMAGE_NAME}:latest"
docker push "${BASE_IMAGE_NAME}:latest"

docker pull "$AUTH_SERVER_IMAGE_NAME" || true
docker build --pull --cache-from "$AUTH_SERVER_IMAGE_NAME" --tag "$AUTH_SERVER_IMAGE_NAME" auth-server/
docker tag "$AUTH_SERVER_IMAGE_NAME" "${AUTH_SERVER_IMAGE_NAME}:latest"
docker push "${AUTH_SERVER_IMAGE_NAME}:latest"

docker pull "$GATEWAY_IMAGE_NAME" || true
docker build --pull --cache-from "$GATEWAY_IMAGE_NAME" --tag "$GATEWAY_IMAGE_NAME" gateway/
docker tag "$GATEWAY_IMAGE_NAME" "${GATEWAY_IMAGE_NAME}:latest"
docker push "${GATEWAY_IMAGE_NAME}:latest"

docker pull "$ORDERS_IMAGE_NAME" || true
docker build --pull --cache-from "$ORDERS_IMAGE_NAME" --tag "$ORDERS_IMAGE_NAME" orders/
docker tag "$ORDERS_IMAGE_NAME" "${ORDERS_IMAGE_NAME}:latest"
docker push "${ORDERS_IMAGE_NAME}:latest"

docker pull "$BILLING_IMAGE_NAME" || true
docker build --pull --cache-from "$BILLING_IMAGE_NAME" --tag "$BILLING_IMAGE_NAME" billing/
docker tag "$BILLING_IMAGE_NAME" "${BILLING_IMAGE_NAME}:latest"
docker push "${BILLING_IMAGE_NAME}:latest"

docker pull "$WAREHOUSE_IMAGE_NAME" || true
docker build --pull --cache-from "$WAREHOUSE_IMAGE_NAME" --tag "$WAREHOUSE_IMAGE_NAME" warehouse/
docker tag "$WAREHOUSE_IMAGE_NAME" "${WAREHOUSE_IMAGE_NAME}:latest"
docker push "${WAREHOUSE_IMAGE_NAME}:latest"
