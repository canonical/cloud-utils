PREFIX = $(DESTDIR)/usr
BIN_D = $(PREFIX)/bin

BINS = uec-publish-image uec-publish-tarball uec-resize-image

build:
	true

install:
	mkdir -p $(BIN_D) && chmod 755 $(BIN_D)
	for f in $(BINS); do \
	   cp $$f $(BIN_D) && chmod 755 $(BIN_D)/$${f##*/} || \
	   exit 1; done

clean:
	true
