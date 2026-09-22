#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
set -eu
cd "$(dirname "$0")/.."
: "${FLATC:=flatc}"
: "${PROTOC:=protoc}"
"$FLATC" --go --gen-all --go-namespace fb \
  --go-module-name github.com/mprammer/vortex-go/internal/wire \
  -I schemas/flatbuffers -o internal/wire \
  schemas/flatbuffers/vortex-file/footer.fbs \
  schemas/flatbuffers/vortex-dtype/dtype.fbs
"$PROTOC" --proto_path=schemas/proto --go_out=. \
  --go_opt=module=github.com/mprammer/vortex-go \
  schemas/proto/dtype.proto schemas/proto/scalar.proto
python3 - <<'PYGEN'
from pathlib import Path
for path in Path("internal/wire/fb").glob("*.go"):
    source = path.read_text()
    if source.startswith("// Code generated"):
        path.write_text("// SPDX-License-Identifier: Apache-2.0\n"
                        "// SPDX-FileCopyrightText: Copyright the Vortex contributors\n\n" + source)
PYGEN
gofmt -w internal/wire
