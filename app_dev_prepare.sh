#!/bin/sh

ROOT=$(dirname $0)

# mq import
(
cat << EOF
package lib

import (
)

EOF
) > $ROOT/apps/mq/lib/import_gen.go

