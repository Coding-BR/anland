#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NDK_DIR="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-}}"
BUILD_DIR="$SCRIPT_DIR/build_daemon_android"

if [ -z "$NDK_DIR" ]; then
    for candidate in \
        "$ANDROID_HOME/ndk/30.0.14904198" \
        "$ANDROID_SDK_ROOT/ndk/30.0.14904198" \
        "$SCRIPT_DIR/build_tools/android-sdk/ndk/29.0.13113456" \
        "/mnt/c/Users/$USER/AppData/Local/Android/Sdk/ndk/30.0.14904198" \
        "/mnt/c/Users/adriano/AppData/Local/Android/Sdk/ndk/30.0.14904198"; do
        if [ -f "$candidate/build/cmake/android.toolchain.cmake" ]; then
            NDK_DIR="$candidate"
            break
        fi
    done
fi

if [ ! -f "$NDK_DIR/build/cmake/android.toolchain.cmake" ]; then
    echo "Android NDK not found at: $NDK_DIR" >&2
    echo "Set ANDROID_NDK_HOME or install the bundled NDK." >&2
    exit 1
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" \
    -DCMAKE_TOOLCHAIN_FILE="$NDK_DIR/build/cmake/android.toolchain.cmake" \
    -DANDROID_ABI=arm64-v8a \
    -DANDROID_PLATFORM=android-30 \
    -DCMAKE_BUILD_TYPE=Release

cmake --build "$BUILD_DIR" --target display_daemon -j$(nproc)

echo "Built: $BUILD_DIR/display_daemon"
