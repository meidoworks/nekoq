#!/bin/sh
find . -name "*.go" | xargs -L 1 wc -l | awk '{print $1}' | while read num; do total=$((total+num)); echo $total; done
