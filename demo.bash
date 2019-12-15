#!/usr/bin/env bash

set -eu
# Generate a series of encrypted ZFS send streams
POOLIMAGE="/tmp/zquashtest.img"
POOL="zquashtest"
DS="$POOL/ds"
MOUNTPOINT="/zquashtest/ds"
export ZS_LSM_ROOT="/tmp/zquash_root"

fallocate -l200M "$POOLIMAGE"
zpool create "$POOL" "$POOLIMAGE"
echo zfs create -o encryption=on -o keyformat=passphrase "$DS"
zfs create -o encryption=on -o keyformat=passphrase "$DS"

echo "initial" > "$MOUNTPOINT"/data
zfs snapshot "$DS"@a
echo "anotherfile" > "$MOUNTPOINT"/data2
zfs snapshot "$DS"@b
echo "appendtodata2" >> "$MOUNTPOINT"/data2
zfs snapshot "$DS"@c
rm "$MOUNTPOINT"/data

zfs snapshot "$DS"@d
zfs send -w "$DS"@a       > a_full.enc.stream
zfs send -w -i @a "$DS"@b > a_b.enc.stream
zfs send -w -i @b "$DS"@c > b_c.enc.stream
zfs send -w -i @c "$DS"@d > c_d.enc.stream

# Load the streams into the zquash database
cargo run -- load   a_b.enc.stream
cargo run -- load   b_c.enc.stream
# squash a->b and b->c into a->c
cargo run -- squash b_c.enc.stream a_b.enc.stream a_c_squashed.enc.stream
# write a->c out to a file
cargo run -- dump   a_c_squashed.enc.stream > a_c_squashed.enc.stream

# demonstrate that our "synthetic" a->c squash is a valid zfs send stream
# by receiving it into a clone of $DS@a and comparing to a clone of $DS@c

zfs send -w "$DS"@c | zfs recv "$DS"_at_c_zfs_send
zfs send -w "$DS"@a | zfs recv "$DS"_at_c_zquashed
zfs recv "$DS"_at_c_zquashed  < a_c_squashed.enc.stream

echo zfs load-key "$DS"_at_c_zfs_send
zfs load-key "$DS"_at_c_zfs_send
zfs mount "$DS"_at_c_zfs_send
echo zfs load-key "$DS"_at_c_zquashed
zfs load-key "$DS"_at_c_zquashed
zfs mount "$DS"_at_c_zquashed

diff -r "$(zfs get -H -o value mountpoint "${DS}_at_c_zfs_send" )" \
        "$(zfs get -H -o value mountpoint "${DS}_at_c_zquashed" )"

# cleanup
# zfs destroy "$POOL"
# rm -rf "$ZS_LSM_ROOT"
