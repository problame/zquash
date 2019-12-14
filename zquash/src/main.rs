use std::path::PathBuf;
use structopt::StructOpt;

use std::fs::File;

use quicli::prelude::*;

use bindings::*;
use failure::{format_err, Error, ResultExt};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, BufWriter};

use ::zquash::lsm;

// Add cool slogan for your app here, e.g.:
/// Squash adjacent zfs send streams together
#[derive(StructOpt)]
struct Cli {
    // Quick and easy logging setup you get for free with quicli
    #[structopt(flatten)]
    verbosity: Verbosity,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[allow(non_camel_case_types)]
enum Command {
    load {
        stream: PathBuf,
        name: Option<String>,
    },
    squash {
        a_to_b: String,
        b_to_c: String,
        target: String,
    },
    dump {
        stream: String,
        target: Option<PathBuf>,
    },
    show {
        stream: String,
    },
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("cli")?;

    match args.command {
        Command::load { stream, name } => {
            dotenv::dotenv().ok();
            use std::fs;

            let mut input = File::open(stream.clone()).context("open input stream file")?;
            let mut input = io::BufReader::new(input);
            let name = match name {
                Some(n) => n,
                None => match stream.file_name().unwrap().to_str() {
                    Some(n) => n.to_string(),
                    None => panic!("cannot stream name from stream file name"),
                },
            };

            let root: PathBuf = std::env::var("ZS_LSM_ROOT")
                .context("ZS_LSM_ROOT not set")?
                .into();

            let cfg = lsm::LSMSrvConfig { root_dir: root };
            lsm::read_stream(&cfg, &mut input, name).context("read stream")?;
        }
        Command::squash {
            a_to_b,
            b_to_c,
            target,
        } => {
            dotenv::dotenv().ok();
            use std::fs;

            let root: PathBuf = std::env::var("ZS_LSM_ROOT")
                .context("ZS_LSM_ROOT not set")?
                .into();

            let cfg = lsm::LSMSrvConfig { root_dir: root };
            unsafe { lsm::merge_streams(&cfg, &[a_to_b.as_ref(), b_to_c.as_ref()], target)? };
        }
        Command::dump { stream, target } => {
            dotenv::dotenv().ok();

            let root: PathBuf = std::env::var("ZS_LSM_ROOT")
                .context("ZS_LSM_ROOT not set")?
                .into();

            let mut target: Box<dyn io::Write> = match target {
                Some(p) => Box::new(
                    File::create(p.clone()).context(format!("create output file {:?}", p))?,
                ),
                None => Box::new(std::io::stdout()),
            };

            let cfg = lsm::LSMSrvConfig { root_dir: root };

            unsafe { lsm::write_stream(&cfg, stream, &mut target) }?;
        }
        Command::show { stream } => {
            dotenv::dotenv().ok();

            let root: PathBuf = std::env::var("ZS_LSM_ROOT")
                .context("ZS_LSM_ROOT not set")?
                .into();

            let cfg = lsm::LSMSrvConfig { root_dir: root };

            unsafe { lsm::show(&cfg, &stream) };
        }
    }

    Ok(())
}
