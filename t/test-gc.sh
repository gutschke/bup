#!/usr/bin/env bash
. ./wvtest-bup.sh

set -o pipefail

top="$(WVPASS pwd)" || exit $?
tmpdir="$(WVPASS wvmktempdir)" || exit $?

export BUP_DIR="$tmpdir/bup"
export GIT_DIR="$tmpdir/bup"

GC_OPTS=--unsafe

bup() { "$top/bup" "$@"; }
compare-trees() { "$top/t/compare-trees" "$@"; }

WVPASS cd "$tmpdir"
WVPASS bup init


WVSTART "gc (unchanged repo)"

WVPASS mkdir src-1
WVPASS bup random 1k > src-1/1
WVPASS bup index src-1
WVPASS bup save --strip -n src-1 src-1

WVPASS bup gc $GC_OPTS -v

WVPASS bup restore -C "$tmpdir/restore" /src-1/latest
WVPASS compare-trees src-1/ "$tmpdir/restore/latest/"


WVSTART "gc (unchanged, new branch)"

WVPASS mkdir src-2
WVPASS bup random 10M > src-2/1
WVPASS bup index src-2
WVPASS bup save --strip -n src-2 src-2

WVPASS bup gc $GC_OPTS -v

WVPASS rm -r "$tmpdir/restore"
WVPASS bup restore -C "$tmpdir/restore" /src-1/latest
WVPASS compare-trees src-1/ "$tmpdir/restore/latest/"

WVPASS rm -r "$tmpdir/restore"
WVPASS bup restore -C "$tmpdir/restore" /src-2/latest
WVPASS compare-trees src-2/ "$tmpdir/restore/latest/"


WVSTART "gc (removed branch)"

size_before=$(WVPASS du -k -s "$BUP_DIR" | WVPASS cut -f1) || exit $?
WVPASS rm "$BUP_DIR/refs/heads/src-2"
WVPASS bup gc $GC_OPTS -v
size_after=$(WVPASS du -k -s "$BUP_DIR" | WVPASS cut -f1) || exit $?

WVPASS [ "$size_before" -gt 5000 ]
WVPASS [ "$size_after" -lt 500 ]

WVPASS rm -r "$tmpdir/restore"
WVPASS bup restore -C "$tmpdir/restore" /src-1/latest
WVPASS compare-trees src-1/ "$tmpdir/restore/latest/"

WVPASS rm -r "$tmpdir/restore"
WVFAIL bup restore -C "$tmpdir/restore" /src-2/latest
 

WVPASS mkdir src-ab-clean src-ab-clean/a src-ab-clean/b
WVPASS bup random 1k > src-ab-clean/a/1
WVPASS bup random 10M > src-ab-clean/b/1


WVSTART "gc (rewriting)"

WVPASS rm -rf "$BUP_DIR"
WVPASS bup init
WVPASS rm -rf src-ab
WVPASS cp -pPR src-ab-clean src-ab

WVPASS bup index src-ab
WVPASS bup save --strip -n src-ab src-ab
WVPASS bup index --clear
WVPASS bup index src-ab
WVPASS bup save -vvv --strip -n a src-ab/a

size_before=$(WVPASS du -k -s "$BUP_DIR" | WVPASS cut -f1) || exit $?
WVPASS rm "$BUP_DIR/refs/heads/src-ab"
WVPASS bup gc $GC_OPTS -v
size_after=$(WVPASS du -k -s "$BUP_DIR" | WVPASS cut -f1) || exit $?

WVPASS [ "$size_before" -gt 5000 ]
WVPASS [ "$size_after" -lt 500 ]

WVPASS rm -r "$tmpdir/restore"
WVPASS bup restore -C "$tmpdir/restore" /a/latest
WVPASS compare-trees src-ab/a/ "$tmpdir/restore/latest/"

WVPASS rm -r "$tmpdir/restore"
WVFAIL bup restore -C "$tmpdir/restore" /src-ab/latest


WVSTART "gc (save -r after repo rewriting)"

WVPASS rm -rf "$BUP_DIR"
WVPASS bup init
WVPASS bup -d bup-remote init
WVPASS rm -rf src-ab
WVPASS cp -pPR src-ab-clean src-ab

WVPASS bup index src-ab
WVPASS bup save -r :bup-remote --strip -n src-ab src-ab
WVPASS bup index --clear
WVPASS bup index src-ab
WVPASS bup save -r :bup-remote -vvv --strip -n a src-ab/a

size_before=$(WVPASS du -k -s bup-remote | WVPASS cut -f1) || exit $?
WVPASS rm bup-remote/refs/heads/src-ab
WVPASS bup -d bup-remote gc $GC_OPTS -v
size_after=$(WVPASS du -k -s bup-remote | WVPASS cut -f1) || exit $?

WVPASS [ "$size_before" -gt 5000 ]
WVPASS [ "$size_after" -lt 500 ]

WVPASS rm -rf "$tmpdir/restore"
WVPASS bup -d bup-remote restore -C "$tmpdir/restore" /a/latest
WVPASS compare-trees src-ab/a/ "$tmpdir/restore/latest/"

WVPASS rm -r "$tmpdir/restore"
WVFAIL bup -d bup-remote restore -C "$tmpdir/restore" /src-ab/latest

# Make sure a post-gc index/save that includes gc-ed data works
WVPASS bup index src-ab
WVPASS bup save -r :bup-remote --strip -n src-ab src-ab
WVPASS rm -r "$tmpdir/restore"
WVPASS bup -d bup-remote restore -C "$tmpdir/restore" /src-ab/latest
WVPASS compare-trees src-ab/ "$tmpdir/restore/latest/"


WVSTART "gc (bup on after repo rewriting)"

WVPASS rm -rf "$BUP_DIR"
WVPASS bup init
WVPASS rm -rf src-ab
WVPASS cp -pPR src-ab-clean src-ab

WVPASS bup on - index src-ab
WVPASS bup on - save --strip -n src-ab src-ab
WVPASS bup index --clear
WVPASS bup on - index src-ab
WVPASS bup on - save -vvv --strip -n a src-ab/a

size_before=$(WVPASS du -k -s "$BUP_DIR" | WVPASS cut -f1) || exit $?
WVPASS rm "$BUP_DIR/refs/heads/src-ab"
WVPASS bup gc $GC_OPTS -v
size_after=$(WVPASS du -k -s "$BUP_DIR" | WVPASS cut -f1) || exit $?

WVPASS [ "$size_before" -gt 5000 ]
WVPASS [ "$size_after" -lt 500 ]

WVPASS rm -r "$tmpdir/restore"
WVPASS bup restore -C "$tmpdir/restore" /a/latest
WVPASS compare-trees src-ab/a/ "$tmpdir/restore/latest/"

WVPASS rm -r "$tmpdir/restore"
WVFAIL bup restore -C "$tmpdir/restore" /src-ab/latest

# Make sure a post-gc index/save that includes gc-ed data works
WVPASS bup on - index src-ab
WVPASS bup on - save --strip -n src-ab src-ab
WVPASS rm -r "$tmpdir/restore"
WVPASS bup restore -C "$tmpdir/restore" /src-ab/latest
WVPASS compare-trees src-ab/ "$tmpdir/restore/latest/"


WVPASS rm -rf "$tmpdir"
