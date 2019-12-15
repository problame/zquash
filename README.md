# zquash

zquash is a tool for squashing adjacent incremental ZFS send streams **without using ZFS**.

The code base is **nothing but a proof-of-concept**, very hacky, and not suitable for production.

zquash was written by [Anton Schirg](https://github.com/antonxy) and [Christian Schwarz](https://cschwarz.com) in Feb-Apr 2019 based on ZFS on Linux 0.8-rc?.
It was udpated to work with 0.8 release in Dec 2019.

## Limitations

* Only works with ZoL 0.8 encrypted streams (`-w` flag, see `demo.bash`)
* Only tested for adjacent _incremental_ streams, not for squashing a full + incremental stream

## Building

Build zquash (by example on Ubuntu 19.10):
```bash
# install rust stable (tested on 1.39), e.g. using rustup.rs
apt install clang libzfslinux-dev libnvpair1linux
# clone this repo, change to root of repo
cargo build
```

## Demo

Read & understand the shell script `./demo.bash`, then run it as **root**.
