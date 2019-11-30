use std::{path::PathBuf, str::FromStr};
use structopt::StructOpt;

pub trait TypeExtension {
    fn extension(&self) -> &'static str;
}

#[derive(Debug, Copy, Clone)]
pub enum CompressionType {
    Gzip,
    Bzip,
    Detect,
    None,
}

impl TypeExtension for CompressionType {
    fn extension(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::Bzip => ".bz",
            Self::Gzip => ".gz",
            Self::Detect => {
                panic!("No reasonable extension for CompressionType::Detect");
            }
        }
    }
}

#[derive(StructOpt, Debug)]
pub struct Config {
    #[structopt(
        short = "g",
        long = "group-col",
        help = "The zero based column with values that must remain together.  \
                To work properly the file must already be sorted by this column"
    )]
    pub group_column: Option<usize>,

    #[structopt(
        short = "n",
        long = "num-rows",
        help = "The maximum number of rows to put in each file.  If we're \
                grouping rows it can be more than this."
    )]
    pub max_rows: usize,

    #[structopt(long = "stdin", help = "Directs csv-split to read from standard input")]
    pub stdin: bool,

    #[structopt(
        short = "i",
        long = "input-compression",
        help = "Treat input data as 'g'zipped, 'b'zipped, or 'd'etect",
        default_value = ""
    )]
    pub input_compression: CompressionType,

    #[structopt(
        short = "z",
        long = "output-compression",
        help = "If present, each file will be compressed when written",
        default_value = ""
    )]
    pub output_compression: CompressionType,

    #[structopt(
        short = "t",
        long = "trigger",
        help = "A command to execute each time a csv file is written."
    )]
    pub trigger: Option<String>,

    #[structopt(
        short = "d",
        long = "header",
        help = "Treat the first row as a header which will be injected into
                each split file."
    )]
    pub headers: bool,

    #[structopt(
        short = "a",
        long = "suffix-length",
        help = "Generate suffixes of length N",
        default_value = "2"
    )]
    pub suffix_len: usize,

    #[structopt(long = "async", help = "Whether to use sync or async writer")]
    pub bg_writer: bool,

    #[structopt(help = "Our input filename to process")]
    pub file: PathBuf,

    #[structopt(
        help = "Output directory (defaults to current directory)",
        default_value = "."
    )]
    pub out_path: PathBuf,
}

impl FromStr for CompressionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "" => Ok(Self::None),
            "g" | "gzip" => Ok(Self::Gzip),
            "b" | "bzip" => Ok(Self::Bzip),
            "d" | "detect" => Ok(Self::Detect),
            _ => Err(format!(
                "Unknown compression format '{}': 'gzip', 'bzip', 'detect', are supported",
                s
            )),
        }
    }
}
