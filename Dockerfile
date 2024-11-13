# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Yuyi Guo, <yuyi@fnal.gov>, 2021


ARG WDIR='/data'

FROM golang:latest as go-builder
ARG WDIR

# build procedure
#ENV WDIR=/data
WORKDIR ${WDIR}
ENV GOPATH=/data/gopath
ARG CGO_ENABLED=0

# build Rucio tracer
WORKDIR ${WDIR} 
RUN git clone https://github.com/dmwm/rucio-tracers.git RucioTracers

WORKDIR ${WDIR}/RucioTracers/stompserver
RUN make

FROM alpine:3.20.3
ARG WDIR
COPY --from=go-builder ${WDIR}/RucioTracers/stompserver/RucioTracer /data/
RUN mkdir -p /data/run && mkdir -p /data/etc
COPY --from=go-builder ${WDIR}/RucioTracers/run.sh /data/run/
COPY --from=go-builder ${WDIR}/RucioTracers/run-swpop.sh /data/run/
COPY --from=go-builder ${WDIR}/RucioTracers/run-xrootd.sh /data/run/
COPY --from=go-builder ${WDIR}/RucioTracers/etc/ruciositemap.json /data/etc/
COPY --from=go-builder ${WDIR}/RucioTracers/etc/domainsitemap.txt /data/etc/
RUN chmod +x /data/run/*
