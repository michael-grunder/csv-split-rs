use crate::config::CompressionType;
use crate::{
    config::Config,
    file::{BackgroundWriter, Decoder, SplitWriter},
};
use async_std::task;
use iou::IoUring;
use structopt::StructOpt;

mod config;
mod file;
mod io;

use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::{BufRead, BufReader, IoSlice, IoSliceMut},
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
};

fn iou_split(filename: &str, nfiles: usize) {
    let mut iou = iou::IoUring::new(4096).unwrap();
    let (mut sq, mut cq, _) = iou.queues();

    let mut writes = 0;
    let mut offset = 0;
    let mut n = 0;

    let mut files = vec![];
    let mut fds = vec![];

    for fid in 0..nfiles {
        let file = File::create(&format!("/tmp/{}.txt", fid)).unwrap();
        let fd = file.as_raw_fd();

        files.push(file);
        fds.push(fd);
    }

    let mut boxes = vec![];
    let mut offsets = HashMap::new();
    let bytes = std::fs::read_to_string(filename)
        .unwrap()
        .as_bytes()
        .to_vec();

    for chunk in bytes.chunks(128) {
        match sq.next_sqe() {
            Some(mut sqe) => unsafe {
                let buffer = Box::new([IoSlice::new(chunk)]);
                sqe.prep_write_vectored(
                    fds[n % nfiles],
                    &*buffer,
                    *offsets.get(&fds[n % nfiles]).or(Some(&0)).unwrap(),
                );
                *offsets.entry(fds[n % nfiles]).or_insert(0) += buffer[0].len();

                offset += buffer[0].len();
                boxes.push(buffer);
                writes += 1;
                n += 1;
            },
            None => {
                while writes > 0 {
                    let sq = cq.wait_for_cqe().unwrap();
                    let nwritten = sq.result().unwrap();
                    println!("[{}]: {} bytes", writes, nwritten);
                    writes -= 1;
                }
            }
        }
    }

    while writes > 0 {
        let cqe = cq.wait_for_cqe().unwrap();
        let id = cqe.user_data() as usize;
        let n = cqe.result().unwrap();
        println!("[{}]: Final {} bytes", id, n);
        writes -= 1;
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let nfiles: usize = args[2].parse().expect("Can't parse number");
    iou_split(&args[1], nfiles);
    Ok(())
}
