NAME = cloud-utils
libdir = /usr/share/$(NAME)
LIBDIR = $(DESTDIR)$(libdir)
BINDIR = $(DESTDIR)/usr/bin
MANDIR = $(DESTDIR)/usr/share/man/man1
DOCDIR = $(DESTDIR)/usr/share/doc/$(NAME)
KEYDIR = $(DESTDIR)/usr/share/keyrings

binprogs := $(subst bin/,,$(wildcard bin/*))
manpages := $(subst man/,,$(wildcard man/*.1))

build: ubuntu-cloudimg-keyring.gpg
	echo manpages=$(manpages)
	python setup.py build

install:
	mkdir -p "$(BINDIR)" "$(DOCDIR)" "$(MANDIR)" "$(KEYDIR)"
	cd bin && install $(binprogs) "$(BINDIR)"
	cd man && install $(manpages) "$(MANDIR)/" --mode=0644
	install -m 0644 ubuntu-cloudimg-keyring.gpg $(KEYDIR)
	python setup.py install --install-layout=deb --root $(CURDIR)/debian/cloud-utils

ubuntu-cloudimg-keyring.gpg: ubuntu-cloudimg-keyring.gpg.b64
	grep -v "^#" "$<" | base64 --decode > "$@" || { rm "$@"; exit 1; }

clean:
	:

uninstall:
	cd "$(BINDIR)" && rm -f $(binprogs) || :
	cd "$(MANDIR)" && rm -f $(manpages) || :
