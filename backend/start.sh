#!/bin/bash
# Find nix-installed X11/GL libs and expose them to mediapipe at runtime
NIX_LIB_PATHS=$(find /nix/store -maxdepth 6 \( \
  -name "libxcb.so*" -o \
  -name "libX11.so*" -o \
  -name "libXext.so*" -o \
  -name "libXrender.so*" -o \
  -name "libXfixes.so*" -o \
  -name "libGL.so*" -o \
  -name "libglib-2.0.so*" \
\) -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ':')

export LD_LIBRARY_PATH="${NIX_LIB_PATHS}${LD_LIBRARY_PATH:-}"
exec uvicorn main:app --host 0.0.0.0 --port $PORT
