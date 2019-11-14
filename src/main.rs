use crate::{
    config::Config,
    file::{Reader, SplitWriter},
};
use csv::WriterBuilder;
use std::{error::Error, fs::File, path::PathBuf};
use structopt::StructOpt;

mod config;
mod file;

fn _open_file(cfg: &Config) -> csv::Reader<Reader<File>> {
    match Reader::open_csv(&cfg.file, cfg.headers, cfg.input_compression) {
        Ok(rdr) => rdr,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(-1);
        }
    }
}

fn reader(cfg: &Config) -> Box<dyn std::io::Read> {
    if cfg.stdin {
        Box::new(Reader::stdin())
    } else {
        Box::new(Reader::open(&cfg.file, cfg.input_compression).unwrap())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cfg: Config = StructOpt::from_args();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader(&cfg));

    let prefix = PathBuf::from(&cfg.file.file_name().unwrap());

    let mut wtr = SplitWriter::new(
        &cfg.out_path,
        &prefix,
        cfg.max_rows,
        cfg.output_compression,
        cfg.group_column,
        &cfg.trigger,
    )?;

    for row in rdr.records().filter_map(|r| r.ok()) {
        wtr.write_record(&row)?;
    }

    Ok(())
}
