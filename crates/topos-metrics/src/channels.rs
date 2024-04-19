pub mod mpsc {
    use futures::{FutureExt, TryFutureExt};
    use prometheus::IntGauge;
    use tokio::sync::mpsc::error::TrySendError;
    pub mod error {
        pub use tokio::sync::mpsc::error::*;
    }
    #[derive(Debug)]
    pub struct Sender<T> {
        inner: tokio::sync::mpsc::Sender<T>,
        gauge: IntGauge,
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                gauge: self.gauge.clone(),
            }
        }
    }

    impl<T> Sender<T> {
        pub async fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
            self.inner
                .send(value)
                .inspect_ok(|_| self.gauge.inc())
                .await
        }

        pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
            self.inner
                .try_send(message)
                // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
                .map(|val| {
                    self.gauge.inc();
                    val
                })
        }
    }

    pub struct Receiver<T> {
        inner: tokio::sync::mpsc::Receiver<T>,
        gauge: IntGauge,
    }

    impl<T> Receiver<T> {
        pub fn inner(self) -> tokio::sync::mpsc::Receiver<T> {
            self.inner
        }

        pub async fn recv(&mut self) -> Option<T> {
            self.inner
                .recv()
                .inspect(|opt| {
                    if opt.is_some() {
                        self.gauge.dec();
                    }
                })
                .await
        }

        pub fn try_recv(&mut self) -> Result<T, tokio::sync::mpsc::error::TryRecvError> {
            self.inner.try_recv().map(|val| {
                self.gauge.dec();
                val
            })
        }
    }

    pub fn channel<T>(size: usize, gauge: &IntGauge) -> (Sender<T>, Receiver<T>) {
        // Reset the gauge
        gauge.set(0);

        let (sender, receiver) = tokio::sync::mpsc::channel(size);

        (
            Sender {
                inner: sender,
                gauge: gauge.clone(),
            },
            Receiver {
                inner: receiver,
                gauge: gauge.clone(),
            },
        )
    }
}

