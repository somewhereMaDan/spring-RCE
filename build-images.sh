#!/bin/bash

# build only if the images don't already exists

build_if_missing(){
    local IMAGE_NAME=$1
    local DOCKERFILE_PATH=$2

    if docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
        echo "✅ $IMAGE_NAME already exists, skipping build."
    else
        echo "🔨 Building $IMAGE_NAME..."
        docker build -t "$IMAGE_NAME" "$DOCKERFILE_PATH"
    fi
}

build_if_missing "python-runner" "./docker/python"
build_if_missing "cpp-runner"    "./docker/cpp"
build_if_missing "java-runner"   "./docker/java"

echo "🚀 All compiler images ready."