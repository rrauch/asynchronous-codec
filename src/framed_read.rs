use super::fuse::Fuse;
use super::Decoder;

use bytes::{BufMut, BytesMut};
use futures_sink::Sink;
use futures_util::io::{AsyncBufRead, AsyncRead};
use futures_util::ready;
use futures_util::stream::{Stream, TryStreamExt};
use pin_project_lite::pin_project;
use std::io;
use std::marker::Unpin;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A `Stream` of messages decoded from an `AsyncRead`.
///
/// # Example
/// ```
/// use asynchronous_codec::{BytesCodec, FramedRead};
/// use futures::TryStreamExt;
/// use bytes::{Bytes};
///
/// let buf = [3u8; 3];
/// let mut framed = FramedRead::new(&buf[..], BytesCodec);
///
/// # futures::executor::block_on(async move {
/// if let Some(bytes) = framed.try_next().await? {
///     assert_eq!(bytes, Bytes::copy_from_slice(&buf[..]));
/// }
/// # Ok::<_, std::io::Error>(())
/// # }).unwrap();
/// ```
#[derive(Debug)]
pub struct FramedRead<T, D, R = AsyncReadStrategy> {
    inner: FramedRead2<Fuse<T, D>, R>,
}

impl<T, D, R> Deref for FramedRead<T, D, R> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner.inner
    }
}

impl<T, D, R> DerefMut for FramedRead<T, D, R> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner.inner
    }
}

impl<T, D, R> FramedRead<T, D, R> {
    /// Consumes the `FramedRead`, returning its parts such that a
    /// new `FramedRead` may be constructed, possibly with a different decoder.
    ///
    /// See also [`FramedRead::from_parts`].
    pub fn into_parts(self) -> FramedReadParts<T, D> {
        let (fuse, buffer) = self.inner.into_parts();
        FramedReadParts {
            io: fuse.t,
            decoder: fuse.u,
            buffer,
            _priv: (),
        }
    }

    /// Consumes the `FramedRead`, returning its underlying I/O stream.
    ///
    /// Note that data that has already been read but not yet consumed
    /// by the decoder is dropped. To retain any such potentially
    /// buffered data, use [`FramedRead::into_parts()`].
    pub fn into_inner(self) -> T {
        self.into_parts().io
    }

    /// Returns a reference to the underlying decoder.
    ///
    /// Note that care should be taken to not tamper with the underlying decoder
    /// as it may corrupt the stream of frames otherwise being worked with.
    pub fn decoder(&self) -> &D {
        &self.inner.u
    }

    /// Returns a mutable reference to the underlying decoder.
    ///
    /// Note that care should be taken to not tamper with the underlying decoder
    /// as it may corrupt the stream of frames otherwise being worked with.
    pub fn decoder_mut(&mut self) -> &mut D {
        &mut self.inner.u
    }

    /// Returns a reference to the read buffer.
    pub fn read_buffer(&self) -> &BytesMut {
        &self.inner.buffer
    }
}

impl<T, D> FramedRead<T, D, AsyncReadStrategy>
where
    T: AsyncRead,
    D: Decoder,
{
    /// Creates a new `FramedRead` transport with the given `Decoder`.
    pub fn new(inner: T, decoder: D) -> Self {
        Self {
            inner: framed_read_2(Fuse::new(inner, decoder), None),
        }
    }

    /// Creates a new `FramedRead` from [`FramedReadParts`].
    ///
    /// See also [`FramedRead::into_parts`].
    pub fn from_parts(
        FramedReadParts {
            io,
            decoder,
            buffer,
            ..
        }: FramedReadParts<T, D>,
    ) -> Self {
        Self {
            inner: framed_read_2(Fuse::new(io, decoder), Some(buffer)),
        }
    }

    /// Disables zero-initialization of newly allocated buffer capacity.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the underlying `AsyncRead` implementation
    /// will never read from the buffer before writing to it. Violating this
    /// contract results in undefined behavior as uninitialized memory may be read.
    ///
    /// Most well-behaved `AsyncRead` implementations satisfy this requirement,
    /// but some implementations may not.
    ///
    /// # Performance
    ///
    /// Disabling initialization can provide significant performance improvements
    /// for high-throughput scenarios by eliminating memory zeroing overhead.
    pub unsafe fn disable_buffer_initialization(&mut self) {
        self.inner.disable_buffer_initialization()
    }

    /// Sets the buffer capacity for read operations.
    ///
    /// This determines how many bytes will be reserved when the buffer needs
    /// to grow. Larger capacities reduce allocation frequency but use more memory.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.inner.set_capacity(capacity)
    }
}

impl<T, D> FramedRead<T, D, AsyncBufReadStrategy>
where
    T: AsyncBufRead,
    D: Decoder,
{
    /// Creates a new *buffered* `FramedRead` transport with the given `Decoder`.
    pub fn new_buffered(inner: T, decoder: D) -> Self {
        Self {
            inner: framed_read_buffered(Fuse::new(inner, decoder), None),
        }
    }

    /// Creates a new *buffered* `FramedRead` from [`FramedReadParts`].
    ///
    /// See also [`FramedRead::into_parts`].
    pub fn from_parts_buffered(
        FramedReadParts {
            io,
            decoder,
            buffer,
            ..
        }: FramedReadParts<T, D>,
    ) -> Self {
        Self {
            inner: framed_read_buffered(Fuse::new(io, decoder), Some(buffer)),
        }
    }
}

impl<T, D, R> Stream for FramedRead<T, D, R>
where
    T: Unpin,
    D: Decoder,
    R: ReadStrategy<Fuse<T, D>>,
{
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.try_poll_next_unpin(cx)
    }
}

mod private_read_strategy {
    pub trait Sealed {}

    impl Sealed for super::AsyncReadStrategy {}
    impl Sealed for super::AsyncBufReadStrategy {}
}

pub trait ReadStrategy<T>: private_read_strategy::Sealed {
    fn read_into_buffer(
        &mut self,
        reader: Pin<&mut T>,
        buffer: &mut BytesMut,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, io::Error>>;
}

#[derive(Debug)]
pub struct AsyncReadStrategy {
    capacity: usize,
    buffer_init_disabled: bool,
}

impl<T: AsyncRead> ReadStrategy<T> for AsyncReadStrategy {
    fn read_into_buffer(
        &mut self,
        reader: Pin<&mut T>,
        buffer: &mut BytesMut,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, io::Error>> {
        // If the buffer has no more spare capacity, reserve more.
        // This prevents passing a zero-length slice to `poll_read`.
        if buffer.spare_capacity_mut().is_empty() {
            // No spare capacity left, reserve a new chunk of `capacity` bytes.
            buffer.reserve(self.capacity);
            let spare = buffer.spare_capacity_mut();
            if !spare.is_empty() && !self.buffer_init_disabled {
                // Initialize the new capacity to avoid the risk of UB.
                init_buffer(spare);
            }
        }

        // Create a mutable slice pointing to the buffer's spare capacity.
        //
        // SAFETY: This is safe because either:
        // a) a previous call to `init_buffer` has zero-initialized
        //    all spare capacity bytes, or
        // b) buffer initialization was disabled but the caller guarantees the
        //    underlying AsyncRead will not read from the buffer before writing to it.
        let buf = unsafe {
            let chunk = buffer.spare_capacity_mut();
            std::slice::from_raw_parts_mut(chunk.as_mut_ptr() as *mut _, chunk.len())
        };

        let n = ready!(reader.poll_read(cx, buf))?;
        assert!(
            n <= buf.len(),
            "reader returned invalid number of bytes read"
        );

        // SAFETY: The `poll_read` call has filled `n` bytes of the buffer.
        // We can now safely advance the buffer's length to make these bytes
        // available for consumption by the decoder.
        unsafe {
            buffer.advance_mut(n);
        }

        Poll::Ready(Ok(n))
    }
}

#[derive(Debug)]
pub struct AsyncBufReadStrategy;

impl<T: AsyncBufRead> ReadStrategy<T> for AsyncBufReadStrategy {
    fn read_into_buffer(
        &mut self,
        mut reader: Pin<&mut T>,
        buffer: &mut BytesMut,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, io::Error>> {
        let filled_buf = ready!(reader.as_mut().poll_fill_buf(cx))?;
        if filled_buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        buffer.extend_from_slice(filled_buf);
        let n = filled_buf.len();
        reader.consume(n);

        Poll::Ready(Ok(n))
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct FramedRead2<T, R>
    {
        #[pin]
        inner: T,
        buffer: BytesMut,
        strategy: R,
    }
}

impl<T, R> Deref for FramedRead2<T, R> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T, R> DerefMut for FramedRead2<T, R> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

const DEFAULT_CAPACITY: usize = 8 * 1024;

pub fn framed_read_2<T: AsyncRead>(
    inner: T,
    buffer: Option<BytesMut>,
) -> FramedRead2<T, AsyncReadStrategy> {
    let mut buffer = buffer.unwrap_or_else(|| BytesMut::new());
    // Ensure any spare capacity of the supplied buffer is initialized.
    init_buffer(buffer.spare_capacity_mut());
    FramedRead2 {
        inner,
        buffer,
        strategy: AsyncReadStrategy {
            capacity: DEFAULT_CAPACITY,
            buffer_init_disabled: false,
        },
    }
}

pub fn framed_read_buffered<T: AsyncBufRead>(
    inner: T,
    buffer: Option<BytesMut>,
) -> FramedRead2<T, AsyncBufReadStrategy> {
    FramedRead2 {
        inner,
        buffer: buffer.unwrap_or_else(|| BytesMut::new()),
        strategy: AsyncBufReadStrategy,
    }
}

impl<T, R> Stream for FramedRead2<T, R>
where
    T: Decoder + Unpin,
    R: ReadStrategy<T>,
{
    type Item = Result<T::Item, T::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        // Attempt to decode a frame from the existing buffer first.
        if let Some(item) = this.inner.decode(&mut this.buffer)? {
            return Poll::Ready(Some(Ok(item)));
        }

        loop {
            // Reading is delegated to the configured ReadStrategy
            let n = ready!(this.strategy.read_into_buffer(
                Pin::new(&mut this.inner),
                &mut this.buffer,
                cx
            ))?;

            let ended = n == 0;

            match this.inner.decode(&mut this.buffer)? {
                Some(item) => return Poll::Ready(Some(Ok(item))),
                None if ended => {
                    if this.buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        match this.inner.decode_eof(&mut this.buffer)? {
                            Some(item) => return Poll::Ready(Some(Ok(item))),
                            None if this.buffer.is_empty() => return Poll::Ready(None),
                            None => {
                                return Poll::Ready(Some(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "bytes remaining in stream",
                                )
                                .into())));
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
    }
}

impl<T, R, I> Sink<I> for FramedRead2<T, R>
where
    T: Sink<I> + Unpin,
{
    type Error = T::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

impl<T, R> FramedRead2<T, R> {
    pub fn into_parts(self) -> (T, BytesMut) {
        (self.inner, self.buffer)
    }

    pub fn buffer(&self) -> &BytesMut {
        &self.buffer
    }
}

impl<T> FramedRead2<T, AsyncReadStrategy> {
    pub unsafe fn disable_buffer_initialization(&mut self) {
        self.strategy.buffer_init_disabled = true;
    }

    pub fn set_capacity(&mut self, capacity: usize) {
        assert!(capacity > 0);
        self.strategy.capacity = capacity
    }
}

/// The parts obtained from (FramedRead::into_parts).
pub struct FramedReadParts<T, D> {
    /// The underlying I/O stream.
    pub io: T,
    /// The frame decoder.
    pub decoder: D,
    /// The buffer of data that has been read from `io` but not
    /// yet consumed by `decoder`.
    pub buffer: BytesMut,
    /// Keep the constructor private.
    _priv: (),
}

impl<T, D> FramedReadParts<T, D> {
    /// Changes the decoder in `FramedReadParts`.
    pub fn map_decoder<E, F>(self, f: F) -> FramedReadParts<T, E>
    where
        E: Decoder,
        F: FnOnce(D) -> E,
    {
        FramedReadParts {
            io: self.io,
            decoder: f(self.decoder),
            buffer: self.buffer,
            _priv: (),
        }
    }
}

#[inline]
fn init_buffer(uninit: &mut [MaybeUninit<u8>]) {
    // SAFETY: We're zero-initializing possibly uninitialized memory.
    // The slice bounds are guaranteed valid and writing to MaybeUninit<u8> as u8 is safe.
    unsafe {
        std::ptr::write_bytes(uninit.as_mut_ptr() as *mut u8, 0x00, uninit.len());
    }
}
