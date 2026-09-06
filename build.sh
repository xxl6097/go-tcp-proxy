#!/usr/bin/env bash

set -e

# 项目信息
APP_NAME="tcpfwd"
CMD_PATH="./cmd/tcpfwd"
OUTPUT_DIR="dist"

# 获取版本信息（从 git tag 或默认值）
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S_UTC')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# 构建标志
LDFLAGS="-s -w"
LDFLAGS="$LDFLAGS -X main.Version=${VERSION}"
LDFLAGS="$LDFLAGS -X main.BuildTime=${BUILD_TIME}"
LDFLAGS="$LDFLAGS -X main.GitCommit=${GIT_COMMIT}"

# 清理并创建输出目录
echo "==> 清理旧的构建文件..."
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# 定义目标平台
platforms=(
    "linux/amd64"
    "linux/arm64"
    "linux/arm"
    "darwin/amd64"
    "darwin/arm64"
    "windows/amd64"
    "windows/arm64"
    "freebsd/amd64"
    "freebsd/arm64"
)

echo "==> 开始编译 ${APP_NAME} 版本: ${VERSION}"
echo "==> 构建时间: ${BUILD_TIME}"
echo "==> Git Commit: ${GIT_COMMIT}"
echo ""

# 编译每个平台
for platform in "${platforms[@]}"; do
    IFS="/" read -r -a array <<< "$platform"
    GOOS="${array[0]}"
    GOARCH="${array[1]}"

    output_name="${APP_NAME}_${GOOS}_${GOARCH}"

    # Windows 需要 .exe 后缀
    if [ "$GOOS" = "windows" ]; then
        output_name="${output_name}.exe"
    fi

    output_path="${OUTPUT_DIR}/${output_name}"

    echo "构建 ${GOOS}/${GOARCH}..."

    # 编译
    env GOOS="$GOOS" GOARCH="$GOARCH" CGO_ENABLED=0 \
        go build -trimpath -ldflags="${LDFLAGS}" \
        -o "${output_path}" "${CMD_PATH}"

    if [ $? -eq 0 ]; then
        # 显示文件大小
        if [ "$GOOS" = "darwin" ]; then
            size=$(ls -lh "${output_path}" | awk '{print $5}')
        else
            size=$(ls -lh "${output_path}" 2>/dev/null | awk '{print $5}' || stat -f%z "${output_path}" 2>/dev/null | numfmt --to=iec || echo "N/A")
        fi
        echo "  ✓ ${output_name} (${size})"
    else
        echo "  ✗ ${output_name} 编译失败"
    fi
done

echo ""
echo "==> 构建完成！输出目录: ${OUTPUT_DIR}/"
echo ""
echo "文件列表:"
ls -lh "${OUTPUT_DIR}/"

# 生成校验和文件
echo ""
echo "==> 生成 SHA256 校验和..."
cd "${OUTPUT_DIR}"
if command -v shasum &> /dev/null; then
    shasum -a 256 * > SHA256SUMS
elif command -v sha256sum &> /dev/null; then
    sha256sum * > SHA256SUMS
fi
cd ..

echo "==> 全部完成！"
