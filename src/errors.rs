use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(
        visibility = "pub",
        display("Could not open file '{}': {}", "filename.display()", "source")
    )]
    OpenFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
