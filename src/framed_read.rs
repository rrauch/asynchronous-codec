use super::fuse::Fuse;
use super::Decoder;

use bytes::{BufMut, BytesMut};
use futures_sink::Sink;
use futures_util::io::AsyncRead;
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
pub struct FramedRead<T, D> {
    inner: FramedRead2<Fuse<T, D>>,
}

impl<T, D> Deref for FramedRead<T, D> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T, D> DerefMut for FramedRead<T, D> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T, D> FramedRead<T, D>
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

impl<T, D> Stream for FramedRead<T, D>
where
    T: AsyncRead + Unpin,
    D: Decoder,
{
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.try_poll_next_unpin(cx)
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct FramedRead2<T> {
        #[pin]
        inner: T,
        buffer: BytesMut,
        capacity: usize,
        buffer_init_disabled: bool,
    }
}

impl<T> Deref for FramedRead2<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for FramedRead2<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

const INITIAL_CAPACITY: usize = 8 * 1024;

pub fn framed_read_2<T>(inner: T, buffer: Option<BytesMut>) -> FramedRead2<T> {
    let buffer = buffer.unwrap_or_else(|| BytesMut::new());
    framed_read_2_with_capacity(inner, buffer, INITIAL_CAPACITY)
}

fn framed_read_2_with_capacity<T>(
    inner: T,
    mut buffer: BytesMut,
    capacity: usize,
) -> FramedRead2<T> {
    // Ensure any spare capacity of the supplied buffer is initialized.
    init_buffer(buffer.spare_capacity_mut());
    FramedRead2 {
        inner,
        capacity,
        buffer,
        buffer_init_disabled: false,
    }
}

impl<T> Stream for FramedRead2<T>
where
    T: AsyncRead + Decoder + Unpin,
{
    type Item = Result<T::Item, T::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        // Attempt to decode a frame from the existing buffer first.
        if let Some(item) = this.inner.decode(&mut this.buffer)? {
            return Poll::Ready(Some(Ok(item)));
        }

        loop {
            // If the buffer has no more spare capacity, reserve more.
            // This prevents passing a zero-length slice to `poll_read`.
            if this.buffer.spare_capacity_mut().is_empty() {
                // No spare capacity left, reserve a new chunk of `this.capacity` bytes.
                this.buffer.reserve(this.capacity);
                let spare = this.buffer.spare_capacity_mut();
                if !spare.is_empty() && !this.buffer_init_disabled {
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
                let chunk = this.buffer.spare_capacity_mut();
                std::slice::from_raw_parts_mut(chunk.as_mut_ptr() as *mut _, chunk.len())
            };

            let n = ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
            assert!(
                n <= buf.len(),
                "reader returned invalid number of bytes read"
            );

            // SAFETY: The `poll_read` call has filled `n` bytes of the buffer.
            // We can now safely advance the buffer's length to make these bytes
            // available for consumption by the decoder.
            unsafe {
                this.buffer.advance_mut(n);
            }

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

impl<T, I> Sink<I> for FramedRead2<T>
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

impl<T> FramedRead2<T> {
    pub fn into_parts(self) -> (T, BytesMut) {
        (self.inner, self.buffer)
    }

    pub fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    pub unsafe fn disable_buffer_initialization(&mut self) {
        self.buffer_init_disabled = true;
    }

    pub fn set_capacity(&mut self, capacity: usize) {
        assert!(capacity > 0);
        self.capacity = capacity
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
