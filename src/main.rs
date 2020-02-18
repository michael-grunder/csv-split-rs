use self::{
    config::{CompressionType, Config},
    file::Decoder,
    io::BufferCache,
};
use csv::ReaderBuilder;
use itertools::Itertools;
use structopt::StructOpt;

use std::{error::Error, fs::File, path::Path};

mod config;
mod file;
mod io;
mod writer;

fn get_reader<P: AsRef<Path>>(
    file: P,
    compression: CompressionType,
) -> Result<csv::Reader<Decoder<File>>, Box<dyn Error>> {
    let compression = match compression {
        CompressionType::None => CompressionType::Detect,
        _ => compression,
    };

    let input = Decoder::open(file.as_ref(), compression)?;
    Ok(ReaderBuilder::new().has_headers(false).from_reader(input))
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Config::from_args();

    let mut on_file = 1;
    let mut on_row = 0;

    let mut rdr = get_reader(&opts.file, opts.input_compression)?;
    let mut buffers = BufferCache::new(opts.queue_depth as usize, opts.buffer_size);
    let mut wtr = buffers.pop();

    if let Some(group_col) = opts.group_column {
        for (_, mut rows) in &rdr
            .byte_records()
            .filter_map(Result::ok)
            .group_by(|row| row[group_col].to_owned())
        {
            if on_row == opts.max_rows || wtr.remaining() <= 1024 {
                wtr = buffers.submit_and_pop(wtr);

                if on_row == opts.max_rows {
                    on_file += 1;
                    buffers.next(&format!("{:05}.csv", on_file));
                    on_row = 0;
                }
            }

            // The first row in a given group, we can split the file here
            let row = rows.next().unwrap();
            wtr.write_row(&row);

            // Now we're in a group which must all stay together
            while let Some(row) = rows.next() {
                if wtr.write_row(&row) <= 1024 {
                    wtr = buffers.submit_and_pop(wtr);

                    on_file += 1;
                    buffers.next(&format!("{:05}.csv", on_file));
                    on_row = 0;
                }
            }
        }
    } else {
        for row in rdr.byte_records().filter_map(|r| r.ok()) {
            if on_row == opts.max_rows || wtr.remaining() <= 1024 {
                wtr = buffers.submit_and_pop(wtr);

                if on_row == opts.max_rows {
                    on_file += 1;
                    buffers.next(&format!("{:05}.csv", on_file));
                    on_row = 0;
                }
            }

            wtr.write_row(&row);
            on_row += 1;
        }

        if wtr.pos() > 0 {
            buffers.submit(wtr);
        }
    }

    Ok(())
}
