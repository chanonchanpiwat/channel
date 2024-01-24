use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.senders += 1;
        drop(inner);
        Sender {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&mut self, t: T) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.queue.push_back(t);
        drop(inner);
        self.shared.available.notify_one();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.senders -= 1;
        drop(inner);
        self.shared.available.notify_one();
    }
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    buffer: VecDeque<T>,
}

impl<T> Receiver<T> {
    pub fn recv(&mut self) -> Option<T> {
        if let Some(t) = self.buffer.pop_front() {
            return Some(t);
        }
        let mut inner = self.shared.inner.lock().unwrap();
        loop {
            match inner.queue.pop_front() {
                Some(t) => {
                    std::mem::swap(&mut self.buffer, &mut inner.queue);
                    return Some(t);
                }
                None if inner.senders == 0 => return None,
                None => inner = self.shared.available.wait(inner).unwrap(),
            }
        }
    }
}

impl<T> Iterator for Receiver<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.recv()
    }
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    available: Condvar,
}

struct Inner<T> {
    queue: VecDeque<T>,
    senders: usize,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Inner {
        queue: VecDeque::default(),
        senders: 1,
    };

    let shared = Shared {
        inner: Mutex::new(inner),
        available: Condvar::new(),
    };

    let shared = Arc::new(shared);

    (
        Sender {
            shared: shared.clone(),
        },
        Receiver {
            shared: shared.clone(),
            buffer: VecDeque::new(),
        },
    )
}

#[cfg(test)]
mod test {
    use std::thread;

    use super::*;

    #[test]
    fn single_sender_test() {
        let (mut tx, mut rx) = channel::<i32>();
        tx.send(43);
        assert_eq!(rx.recv(), Some(43));
    }

    #[test]
    fn single_sender_drop_test() {
        let (tx, mut rx) = channel::<i32>();
        drop(tx);
        assert_eq!(rx.recv(), None);
    }

    #[test]
    fn multi_send_test() {
        let (mut tx, mut rx) = channel::<i32>();
        let mut tx2 = tx.clone();
        thread::spawn(move || {
            tx.send(1);
        });

        thread::spawn(move || tx2.send(2));

        let f1 = rx.recv();
        let f2 = rx.recv();
        let end = rx.recv();
        assert_eq!(f1, Some(1));
        assert_eq!(f2, Some(2));
        assert_eq!(end, None);
    }
}
