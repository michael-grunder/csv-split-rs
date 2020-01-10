use crate::config::{CompressionType, TypeExtension};
use bzip2::{read::BzDecoder, write::BzEncoder};
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
