# Author: Tom Vanmechelen @ KU Leuven (2024). Apache 2.0 License
#FROM python:alpine
FROM python:3.13.0-bookworm

RUN addgroup app --gid 10000 && adduser app --home /app --uid 10000 --ingroup app
USER app:app

# RUN apk add --update --no-cache --virtual .tmp-build-deps \
#    gcc libc-dev linux-headers postgresql-dev musl-dev zlib zlib-dev \
#    libressl-dev libffi-dev

# RUN mkdir -p /opt/exiftool \
#    && cd /opt/exiftool \
#    && EXIFTOOL_VERSION=`curl -s https://exiftool.org/ver.txt` \
#    && EXIFTOOL_ARCHIVE=Image-ExifTool-${EXIFTOOL_VERSION}.tar.gz \
#    && curl -s -O https://exiftool.org/$EXIFTOOL_ARCHIVE \
#    && CHECKSUM=`curl -s https://exiftool.org/checksums.txt | grep SHA1\(${EXIFTOOL_ARCHIVE} | awk -F'= ' '{print $2}'` \
#    && echo "${CHECKSUM}  ${EXIFTOOL_ARCHIVE}" | /usr/bin/sha1sum -c -s - \
#    && tar xzf $EXIFTOOL_ARCHIVE --strip-components=1 \
#    && rm -f $EXIFTOOL_ARCHIVE \
#    && perl Makefile.PL \
#    && make install \
#    && exiftool -ver 


#RUN mkdir -p /opt/exiftool; cd /opt/exiftool;  
#RUN EXIFTOOL_VERSION=`curl -s https://exiftool.org/ver.txt`; EXIFTOOL_ARCHIVE=Image-ExifTool-${EXIFTOOL_VERSION}.tar.gz; curl -s -O https://exiftool.org/$EXIFTOOL_ARCHIVE;
#RUN CHECKSUM=`curl -s https://exiftool.org/checksums.txt | grep SHA1\(${EXIFTOOL_ARCHIVE} | awk -F'= ' '{print $2}'`; echo "${CHECKSUM} ${EXIFTOOL_ARCHIVE}" | /usr/bin/sha1sum -c -;
#RUN tar xzf $EXIFTOOL_ARCHIVE --strip-components=1; rm -f $EXIFTOOL_ARCHIVE; perl Makefile.PL; make install; exiftool -ver;

#RUN pip install python-irodsclient
#RUN pip install mango-mdschema
#RUN pip install rich
#RUN pip install numpy

WORKDIR /app/mango-ingest
ADD ./src/ ./src


#RUN python -m venv venv 
#RUN . venv/bin/activate
# RUN pip install --editable src
#RUN pip install python-magic
#RUN pip install pyexiftool
#RUN pip install boltons

USER root:root
ENV PATH="/venv/bin:$PATH"
RUN python -m venv /venv; . /venv/bin/activate; pip install --editable src;
RUN pip install mango-mdschema; 


#RUN pip install pyexiftool

#RUN mkdir -p /opt/exiftool; cd /opt/exiftool;  
#RUN EXIFTOOL_VERSION=`curl -s https://exiftool.org/ver.txt`; EXIFTOOL_ARCHIVE=Image-ExifTool-${EXIFTOOL_VERSION}.tar.gz; curl -s -O https://exiftool.org/$EXIFTOOL_ARCHIVE;
#RUN CHECKSUM=`curl -s https://exiftool.org/checksums.txt | grep SHA1\(${EXIFTOOL_ARCHIVE} | awk -F'= ' '{print $2}'`; echo "${CHECKSUM} ${EXIFTOOL_ARCHIVE}" | /usr/bin/sha1sum -c -;
#RUN tar xzf $EXIFTOOL_ARCHIVE --strip-components=1; rm -f $EXIFTOOL_ARCHIVE; perl Makefile.PL; make install; exiftool -ver;

WORKDIR /app/mango-ingest
