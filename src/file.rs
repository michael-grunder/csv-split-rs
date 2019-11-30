use crate::config::{CompressionType, TypeExtension};
use bzip2::{read::BzDecoder, write::BzEncoder};
use crossbeam::channel::{Receiver as CBReceiver, Sender as CBSender};
use csv::{Writer as CsvWriter, WriterBuilder};
use flate2::{read::GzDecoder, write::GzEncoder};
use snafu::{ensure, ErrorCompat, ResultExt, Snafu};
use std::{
    fs::File,
    io::{Read, Result as IoResult, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    thread,
};

pub enum Decoder<R: Read> {
    Plain(R),
    Gzipped(GzDecoder<R>),
    Bzipped(BzDecoder<R>),
}

pub enum Encoder<W: Write> {
    Plain(W),
    Gzipped(GzEncoder<W>),
    Bzipped(BzEncoder<W>),
}

pub struct SplitWriter {
    path: PathBuf,
    prefix: PathBuf,
    max_rows: usize,
    on_file: usize,
    on_row: usize,
    compression: CompressionType,
    group_column: Option<usize>,
    trigger: Option<String>,
    writer: CsvWriter<Encoder<File>>,
    last_value: Option<String>,
}

#[derive(Debug)]
pub struct BackgroundWriter {
    tx: Option<CBSender<csv::StringRecord>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for BackgroundWriter {
    fn drop(&mut self) {
        drop(self.tx.take());

        self.handle
            .take()
            .unwrap()
            .join()
            .expect("Failed to wait on background worker thread");
    }
}

impl BackgroundWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        prefix: P,
        max_rows: usize,
        compression: CompressionType,
        group_column: Option<usize>,
        trigger: &Option<String>,
    ) -> Result<Self> {
        let (tx, rx) = crossbeam::channel::unbounded();

        let mut writer =
            SplitWriter::new(path, prefix, max_rows, compression, group_column, trigger)?;

        Ok(Self {
            tx: Some(tx),
            handle: Some(std::thread::spawn(move || {
                while let Ok(msg) = rx.recv() {
                    writer.write_record(&msg).unwrap();
                }
            })),
        })
    }

    #[inline]
    pub fn write_record(&mut self, row: &csv::StringRecord) -> Result<()> {
        self.tx
            .as_ref()
            .unwrap()
            .send(row.to_owned())
            .map_err(|_| Error::Delivery)
    }
}

//struct SplitFile<W>
//where
//    W: Write,
//{
//    group_column: Option<usize>,
//    group_value: Option<String>,
//    row_count: usize,
//    max_rows: usize,
//    writer: csv::Writer<Encoder<W>>,
//}
//
//impl SplitFile<File> {
//    pub fn new<P: AsRef<Path>>(
//        filename: P,
//        compression: CompressionType,
//        group_column: Option<usize>,
//    ) -> Result<Self> {
//        let writer = csv::WriterBuilder::new()
//            .has_headers(false)
//            .from_writer(Encoder::create(filename, compression)?);
//
//        Ok(Self {
//            group_column,
//            row_count: 0,
//            max_rows: 0,
//            group_value: None,
//            writer,
//        })
//    }
//
//    fn group_value<'a>(&'a self, row: &'a csv::StringRecord) -> Option<&'a str> {
//        if let Some(n) = self.group_column {
//            if row.len() > n {
//                return Some(&row[n]);
//            }
//        }
//
//        None
//    }
//
//    fn check(&mut self, row: &csv::StringRecord) -> bool {
//        if self.row_count >= self.max_rows {
//            match (self.group_value.as_ref(), self.group_value(row)) {
//                (Some(a), Some(b)) => a == b,
//                _ => false,
//            }
//        } else {
//            false
//        }
//    }
//
//    fn push(&mut self, row: &csv::StringRecord) -> Result<()> {
//        if let Some(gc) = self.group_column {
//            self.group_value = row.get(gc).map_or(None, |v| Some(v.to_owned()));
//        }
//
//        self.row_count += 1;
//        self.writer.write_record(row).context(Csv)
//    }
//}

#[derive(Debug)]
struct Trigger {
    cmdstr: String,
    cmd: Command,
}

impl Trigger {
    const SHELL: &'static str = "sh";

    fn file_name<P: AsRef<Path>>(file: P) -> String {
        file.as_ref()
            .file_name()
            .unwrap_or(std::ffi::OsStr::new(""))
            .to_string_lossy()
            .into_owned()
    }

    fn full_path<P: AsRef<Path>>(path: P) -> String {
        std::fs::canonicalize(&path)
            .unwrap_or(path.as_ref().to_path_buf())
            .to_string_lossy()
            .into_owned()
    }

    fn new<P: AsRef<Path>>(cmd: &str, output_file: &P, num_rows: usize) -> Self {
        // Do variable replacements
        let cmdstr = cmd
            .to_owned()
            .replace("{}", &Self::full_path(output_file))
            .replace("{/}", &Self::file_name(output_file))
            .replace("{rows}", &num_rows.to_string());

        // Set up command itself
        let mut cmd = Command::new(Self::SHELL);
        cmd.arg("-c").arg(&cmdstr);

        Self { cmdstr, cmd }
    }

    fn exec(&mut self) -> Result<std::process::Output> {
        self.cmd.output().context(Generic)
    }
}

impl<R> Read for Decoder<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        match self {
            Self::Plain(rdr) => rdr.read(buf),
            Self::Gzipped(rdr) => rdr.read(buf),
            Self::Bzipped(rdr) => rdr.read(buf),
        }
    }
}

impl<W> Write for Encoder<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self {
            Self::Plain(w) => w.write(buf),
            Self::Gzipped(w) => w.write(buf),
            Self::Bzipped(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match self {
            Self::Plain(w) => w.flush(),
            Self::Gzipped(w) => w.flush(),
            Self::Bzipped(w) => w.flush(),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Can't open file {}: {}", filename.display(), source))]
    Open {
        filename: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Can't create file {}: {}", filename.display(), source))]
    Create {
        filename: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("I/O '{}', error in file {} ({})", op, filename.display(), source))]
    Io {
        filename: PathBuf,
        op: &'static str,
        source: std::io::Error,
    },

    #[snafu(display("Generic I/O error: {}", source))]
    Generic { source: std::io::Error },

    #[snafu(display("CSV I/O error: {}", source))]
    Csv { source: csv::Error },

    #[snafu(display("Sync error"))]
    Delivery,
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl<R: Read> Decoder<R> {
    fn gz(rdr: R) -> Self {
        Decoder::Gzipped(GzDecoder::new(rdr))
    }

    fn bz(rdr: R) -> Self {
        Decoder::Bzipped(BzDecoder::new(rdr))
    }

    fn plain(rdr: R) -> Self {
        Decoder::Plain(rdr)
    }

    fn decoder(rdr: R, compression: CompressionType) -> Decoder<R> {
        match compression {
            CompressionType::Gzip => Self::gz(rdr),
            CompressionType::Bzip => Self::bz(rdr),
            CompressionType::None => Self::plain(rdr),
            _ => panic!("Must specify a specific compression type"),
        }
    }
}

impl<R: Read + Seek> Decoder<R> {
    const GZIP_MAGIC_BYTES: [u8; 2] = [0x1F_u8, 0x8B];
    const BZIP_MAGIC_BYTES: [u8; 2] = [b'B', b'Z'];

    fn detect_compression(rdr: &mut R) -> Result<CompressionType> {
        let mut bytes = [0_u8; 2];

        rdr.read_exact(&mut bytes).context(Generic)?;
        rdr.seek(SeekFrom::Start(0)).context(Generic)?;

        match bytes {
            Self::GZIP_MAGIC_BYTES => Ok(CompressionType::Gzip),
            Self::BZIP_MAGIC_BYTES => Ok(CompressionType::Bzip),
            _ => Ok(CompressionType::None),
        }
    }
}

impl Decoder<File> {
    pub fn open<P: AsRef<Path>>(filename: P, compression: CompressionType) -> Result<Self> {
        let filename = filename.as_ref();

        // Open our input file file
        let mut file = File::open(&filename).context(Open { filename })?;

        // Resolve the compression type depending on what was requested or what we detect.
        let ctype = if let CompressionType::Detect = compression {
            Self::detect_compression(&mut file)?
        } else {
            compression
        };

        Ok(Self::decoder(file, ctype))
    }
}

impl Decoder<std::io::Stdin> {
    pub fn stdin() -> Self {
        Self::decoder(std::io::stdin(), CompressionType::None)
    }
}

impl SplitWriter {
    fn file_name<P: AsRef<Path>>(
        path: P,
        prefix: P,
        compression: CompressionType,
        n: usize,
    ) -> PathBuf {
        let mut path = path.as_ref().to_path_buf();
        path.push(prefix.as_ref());

        let suffix = if let Some(ext) = path.extension() {
            format!(
                "{}.{:05}{}",
                ext.to_string_lossy(),
                n,
                compression.extension()
            )
        } else {
            format!("{:05}{}", n, compression.extension())
        };

        path.set_extension(&suffix);

        path
    }

    fn get_writer<P: AsRef<Path>>(
        filename: P,
        compression: CompressionType,
    ) -> Result<csv::Writer<Encoder<File>>> {
        let writer = Encoder::create(filename, compression)?;
        Ok(csv::WriterBuilder::new().from_writer(writer))
    }

    pub fn new<P: AsRef<Path>>(
        path: P,
        prefix: P,
        max_rows: usize,
        compression: CompressionType,
        group_column: Option<usize>,
        trigger: &Option<String>,
    ) -> Result<Self> {
        let filename = Self::file_name(&path, &prefix, compression, 1);
        let writer = Self::get_writer(&filename, compression)?;

        Ok(Self {
            path: PathBuf::from(path.as_ref()),
            prefix: PathBuf::from(prefix.as_ref()),
            compression,
            group_column,
            max_rows,
            on_file: 1,
            on_row: 0,
            trigger: trigger.clone(),
            writer,
            last_value: None,
        })
    }

    fn next_writer(&mut self) -> Result<()> {
        self.on_file += 1;
        self.on_row = 0;

        let filename = Self::file_name(&self.path, &self.prefix, self.compression, self.on_file);
        self.writer = Self::get_writer(&filename, self.compression)?;

        Ok(())
    }

    fn get_trigger(&self) -> Option<Trigger> {
        if let Some(ref trigger) = self.trigger {
            let exec = Trigger::new(
                trigger,
                &Self::file_name(&self.path, &self.prefix, self.compression, self.on_file),
                self.on_row,
            );

            Some(exec)
        } else {
            None
        }
    }

    fn write_split(&mut self) -> Result<()> {
        let trigger = self.get_trigger();

        self.next_writer()?;

        if let Some(mut trigger) = trigger {
            trigger.exec()?;
        }

        Ok(())
    }

    pub fn write_record(&mut self, row: &csv::StringRecord) -> Result<()> {
        if self.on_row >= self.max_rows {
            let in_group = if let Some(gc) = self.group_column {
                if let Some(ref last_value) = self.last_value {
                    row.len() > gc && last_value == &row[gc]
                } else {
                    false
                }
            } else {
                false
            };

            if !in_group {
                if let Some(gc) = self.group_column {
                    self.last_value = row.get(gc).map_or(None, |v| Some(v.to_owned()));
                }

                self.write_split()?;
            }
        } else if let Some(gc) = self.group_column {
            self.last_value = row.get(gc).map_or(None, |v| Some(v.to_owned()));
        };

        self.on_row += 1;
        self.writer.write_record(row).context(Csv)
    }

    pub fn finalize(&mut self) -> Result<()> {
        if self.on_row > 0 {
            self.write_split()?;
        }

        Ok(())
    }
}

impl Drop for SplitWriter {
    fn drop(&mut self) {
        if self.on_row > 0 {
            self.finalize().expect("Failed to finish processing");
        }
    }
}

impl Encoder<File> {
    pub fn create<P: AsRef<Path>>(filename: P, compression: CompressionType) -> Result<Self> {
        let filename = filename.as_ref();

        let file = File::create(&filename).context(Create { filename })?;

        let wtr = match compression {
            CompressionType::Gzip => {
                let encoder = GzEncoder::new(file, flate2::Compression::default());
                Self::Gzipped(encoder)
            }
            CompressionType::Bzip => {
                let encoder = BzEncoder::new(file, bzip2::Compression::Default);
                Self::Bzipped(encoder)
            }
            _ => Self::Plain(file),
        };

        Ok(wtr)
    }
}
