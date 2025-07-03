#!/bin/sh

exec /bin/streaming \
  -port $PORT \
  -segment-size $SEGMENT_SIZE \
  -systems-yaml-dir "$SYSTEMS_YAML_DIR" \
  -models-yaml-dir "$MODELS_YAML_DIR" \
  -queue-dir "$QUEUE_DIR"
