#!/bin/sh

exec /bin/streaming \
  -port $PORT \
  -systems-dir "$SYSTEMS_DIR" \
  -models-dir "$MODELS_DIR"
#   -segment-size $SEGMENT_SIZE \
#   -queue-dir "$QUEUE_DIR"
