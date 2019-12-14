extern crate bindgen;

use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let helpers = Path::new(&dir).join("./helpers");

    // compile helpers lib
    cc::Build::new()
        .file(helpers.join("helpers.c"))
        .flag("-Werror")
        .flag("-Wall")
        .flag("-Wpedantic")
        .compile("helpers");

    let clang_args = [format!("-I{}", helpers.display())];
    let bindings = bindgen::Builder::default()
        .rust_target(bindgen::RustTarget::Nightly)
        .header("wrapper.h")
        .header(helpers.join("helpers.h").to_str().unwrap())
        .clang_args(&clang_args)
        .derive_debug(true)
        .whitelist_recursively(true)
        .whitelist_function("drr_effective_payload_len")
        .whitelist_type("dmu_replay_record")
        .generate()
        .expect("unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("couldn't write bindings!");
}
