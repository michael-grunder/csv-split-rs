use csv::ByteRecord;
use csv_core::Writer as CsvWriter;
use iou::{IoUring, SubmissionFlags};
use std::{
    collections::HashMap,
    convert::{From, TryInto},
    error::Error,
    fs::File,
    io::{self, IoSlice},
    os::unix::io::AsRawFd,
    path::Path,
    pin::Pin,
};

pub trait VectoredWrite {
    fn as_write(&self) -> &[IoSlice];
}

pub struct IouWrites<'a> {
    next_id: u64,
    iou: IoUring,
    writes: HashMap<u64, Box<IouWrite<'a>>>,
    in_flight: u32,
}

pub struct CsvBuffers {
    buffers: Vec<CsvBuffer>,
}

#[derive(Debug)]
pub struct IouWrite<'a> {
    _data: Box<[u8]>,
    write: [IoSlice<'a>; 1],
}

#[derive(Debug)]
pub struct CsvBuffer {
    wtr: csv_core::Writer,
    data: Box<[u8]>,
    pos: usize,
}

pub struct BufferCache<'a> {
    buffers: CsvBuffers,
    writes: IouWrites<'a>,
    files: SplitFileWrapper,
}

#[derive(Debug)]
pub struct FileWrapper {
    file: File,
    fd: i32,
    offset: usize,
}

pub struct SplitFileWrapper(Vec<FileWrapper>);

impl FileWrapper {
    pub fn create<P: AsRef<Path>>(filename: P) -> Self {
        let file = File::create(filename).unwrap();
        let fd = file.as_raw_fd();

        Self {
            file,
            fd,
            offset: 0,
        }
    }

    pub fn c2(n: i32) -> Self {
        Self::create(format!("{:05}.csv", n))
    }
}

impl SplitFileWrapper {
    pub fn new() -> Self {
        Self(vec![FileWrapper::c2(1)])
    }

    pub fn next<P: AsRef<Path>>(&mut self, filename: P) -> i32 {
        let file = FileWrapper::create(filename);
        let id = self.0.len();

        self.0.push(file);

        id.try_into().unwrap()
    }

    pub fn fd_offset(&mut self) -> (i32, &mut usize) {
        let last = self.0.last_mut().unwrap();
        (last.fd, &mut last.offset)
    }
}

impl<'a> IouWrite<'a> {
    // pub fn new(data: Box<[u8]>, len: usize) -> Self {
    //     let dref: &[u8] = unsafe { &*(data.as_ref() as *const _) };
    //     Self {
    //         _data: data,
    //         write: [IoSlice::new(&dref[0..len])],
    //     }
    // }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl VectoredWrite for IouWrite<'_> {
    fn as_write(&self) -> &[IoSlice<'_>] {
        &self.write
    }
}

impl From<CsvBuffer> for IouWrite<'_> {
    fn from(buffer: CsvBuffer) -> Self {
        let dref: &[u8] = unsafe { &*(buffer.data.as_ref() as *const _) };
        Self {
            _data: buffer.data,
            write: [IoSlice::new(&dref[0..buffer.pos])],
        }
    }
}

impl From<IouWrite<'_>> for CsvBuffer {
    fn from(io: IouWrite) -> Self {
        Self {
            wtr: csv_core::Writer::new(),
            data: io._data,
            pos: 0,
        }
    }
}

impl CsvBuffer {
    pub fn zeroed(capacity: usize) -> Self {
        let data = vec![0; capacity].into_boxed_slice();
        Self {
            wtr: CsvWriter::new(),
            data,
            pos: 0,
        }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn write_row(&mut self, row: &csv::ByteRecord) -> usize {
        let mut buf = &mut self.data[self.pos..];
        let len = row.len();

        for c in 0..len - 1 {
            let (_, _, n) = self.wtr.field(&row[c], buf);
            buf = &mut buf[n..];

            let (_, n) = self.wtr.delimiter(buf);
            buf = &mut buf[n..];
        }

        let (_, _, n) = self.wtr.field(&row[len - 1], buf);
        buf = &mut buf[n..];

        let (_, n) = self.wtr.terminator(buf);
        buf = &mut buf[n..];

        let rowlen = buf.as_ptr() as usize - self.data[self.pos..].as_ptr() as usize;
        self.pos += rowlen;

        self.remaining()
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn into_boxed_write<'a>(self) -> Box<IouWrite<'a>> {
        IouWrite::from(self).boxed()
    }
}

impl CsvBuffers {
    pub fn new(count: usize, size: usize) -> Self {
        Self {
            buffers: (0..count).map(|_| CsvBuffer::zeroed(size)).collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }

    pub fn pop(&mut self) -> CsvBuffer {
        self.buffers.pop().unwrap()
    }

    pub fn push<T: Into<CsvBuffer>>(&mut self, value: T) {
        self.buffers.push(value.into());
    }
}

impl<'a> IouWrites<'a> {
    pub fn new(queue_depth: u32) -> Self {
        Self {
            iou: IoUring::new(queue_depth).unwrap(),
            writes: HashMap::new(),
            next_id: 42,
            in_flight: 0,
        }
    }

    pub fn write_and_store(&mut self, fd: i32, buffer: CsvBuffer, offset: usize) -> u64 {
        let (id, io) = (self.next_id, buffer.into_boxed_write());

        let len = io.write[0].len();

        unsafe {
            let mut sq = self.iou.sq();
            let mut sqe = sq.next_sqe().unwrap();
            sqe.prep_write_vectored(fd, &io.write, offset);
            sqe.set_flags(SubmissionFlags::IO_LINK);
            sqe.set_user_data(id);
            self.iou.sq().submit().unwrap();
        }

        if self.writes.insert(id, io).is_some() {
            panic!("Duplicate write ID");
        }

        self.in_flight += 1;
        self.next_id += 1;

        id
    }

    fn recover_buffer(
        writes: &mut HashMap<u64, Box<IouWrite>>,
        cqe: iou::CompletionQueueEvent,
    ) -> CsvBuffer {
        let id = cqe.user_data();
        let len = cqe.result().unwrap();

        let write = *writes.remove(&id).unwrap();
        write.into()
    }

    pub fn recover_buffers(&mut self, buffers: &mut CsvBuffers) {
        if self.in_flight > 0 {
            // Wait for everything in flight but recover the first one
            let cqe = self.iou.wait_for_cqes(self.in_flight as usize).unwrap();
            buffers.push(Self::recover_buffer(&mut self.writes, cqe));
            self.in_flight -= 1;

            while self.in_flight > 0 {
                let cqe = self.iou.wait_for_cqe().unwrap();
                buffers.push(Self::recover_buffer(&mut self.writes, cqe));
                self.in_flight -= 1;
            }
        }
    }
}

impl<'a> Drop for IouWrites<'a> {
    fn drop(&mut self) {
        if self.in_flight > 0 {
            self.iou.wait_for_cqes(self.in_flight as usize).unwrap();
        }
    }
}

impl<'a> BufferCache<'a> {
    pub fn new(count: usize, size: usize) -> Self {
        Self {
            buffers: CsvBuffers::new(count, size),
            writes: IouWrites::new(count as u32),
            files: SplitFileWrapper::new(),
        }
    }

    pub fn pop(&mut self) -> CsvBuffer {
        if self.buffers.is_empty() {
            self.writes.recover_buffers(&mut self.buffers);
        }

        self.buffers.pop()
    }

    pub fn submit(&mut self, buffer: CsvBuffer) -> u64 {
        let (fd, offset) = self.files.fd_offset();
        let len = buffer.pos();
        let rv = self.writes.write_and_store(fd, buffer, *offset);
        *offset += len;

        rv
    }

    pub fn submit_and_pop(&mut self, buffer: CsvBuffer) -> CsvBuffer {
        self.submit(buffer);
        self.pop()
    }

    pub fn next<P: AsRef<Path>>(&mut self, filename: P) {
        self.files.next(filename);
    }
}
