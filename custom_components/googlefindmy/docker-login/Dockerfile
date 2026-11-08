# GoogleFindMy-HA — containerised one-time Google login helper.
#
# This is a Docker wrapper around the *integration's own* standalone CLI
# (custom_components/googlefindmy/main.py), NOT the upstream GoogleFindMyTools
# CLI. It bundles Chrome + a virtual display + noVNC so the one-time Google
# login can be driven from a browser tab, and it writes a secrets.json that is
# byte-for-byte the file the integration consumes.
#
# Attribution: the noVNC/entrypoint container design is adapted from
# sincze/GoogleFindMyTools @ docker-novnc-auth
# (https://github.com/sincze/GoogleFindMyTools/tree/docker-novnc-auth); the
# original CLI is leonboe1/GoogleFindMyTools. This wrapper turns that design
# onto the integration's *own* main.py so the produced secrets.json matches the
# installed integration byte-for-byte. See README.md → Credits.
#
# Run model (M2, runtime mount + standalone):
#   * The integration code is NOT baked into the image. docker-compose.yml
#     bind-mounts it READ-ONLY at a flat path (/app/gfmy), so main.py detects
#     the standalone layout (_standalone=True), injects lightweight
#     homeassistant.* stubs, and runs without a Home Assistant install. The
#     image therefore stays version-independent and always runs the installed
#     integration version.
#   * secrets.json is written to a SEPARATE writable volume (/data) via
#     GOOGLEFINDMY_SECRETS_PATH, so the read-only code mount is never written to
#     and the atomic write stays on a single writable filesystem.
# Only the declared runtime dependencies are baked in (Home Assistant and
# voluptuous are deliberately NOT installed — the standalone path stubs HA and
# never imports the package __init__/config_flow that need voluptuous).
#
# Build context is the integration package dir (custom_components/googlefindmy),
# see docker-compose.yml `build.context: ..`.
FROM selenium/standalone-chrome:latest

USER root
WORKDIR /app

# --- Python dependencies -------------------------------------------------------
# requirements.txt lives inside the integration package (the build context), so
# it is COPYed at build time. The integration *code* is NOT copied (M2): it is  # codespell:ignore copyed
# mounted at run time, which keeps the image independent of the code version.
COPY requirements.txt /app/requirements.txt
# Install setuptools alongside the requirements: undetected-chromedriver 3.5.5
# (the current PyPI top, so a version bump is not available) still imports
# `distutils`, which was removed from the stdlib in Python 3.12 (PEP 632). The
# base image selenium/standalone-chrome:latest has since moved to Python 3.14,
# where the bare import fails (ModuleNotFoundError: distutils) and Chrome never
# starts. setuptools re-provides distutils via its _distutils_hack, so this is a
# dependency fix only, NOT a Chrome/ChromeDriver version pin.
RUN pip3 install --no-cache-dir setuptools -r /app/requirements.txt

# Writable volume mount point for the produced secrets.json (see
# docker-compose.yml → ./data:/data and GOOGLEFINDMY_SECRETS_PATH). Created and
# owned here as a fallback; the bind mount provides the real, host-persisted dir.
RUN mkdir -p /data \
 && chown -R seluser:seluser /app /data \
 && chown -R seluser:seluser /home/seluser

COPY docker-login/entrypoint.sh /opt/bin/gfm-entrypoint.sh
RUN chmod +x /opt/bin/gfm-entrypoint.sh

# openssl is needed at RUNTIME to mint the self-signed noVNC certificate for a
# LAN bind (docker-login/entrypoint.sh, AP-8). It is normally already present in
# the selenium base image; install it defensively so a LAN-bind login can always
# encrypt the viewer. A loopback login never calls openssl, so this only matters
# for the opt-in LAN path.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssl \
 && rm -rf /var/lib/apt/lists/*

# Self-signed TLS override for noVNC (AP-8). Preserve the base image's original
# launcher as start-novnc.base.sh FIRST, so the override can hand off to it
# verbatim on the loopback/tunnel default (no cert -> unchanged path), then
# replace /opt/bin/start-novnc.sh (the path [program:novnc] already runs).
RUN cp -a /opt/bin/start-novnc.sh /opt/bin/start-novnc.base.sh
COPY docker-login/start-novnc.sh /opt/bin/start-novnc.sh
RUN chmod +x /opt/bin/start-novnc.sh

USER seluser
ENTRYPOINT ["/opt/bin/gfm-entrypoint.sh"]
