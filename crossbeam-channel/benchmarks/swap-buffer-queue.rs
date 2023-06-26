use std::{sync::atomic::Ordering::Relaxed, thread};

use crate::channel::channel;

mod channel;
mod message;

const MESSAGES: usize = 5_000_000;
const THREADS: usize = 20;

fn seq(cap: usize) {
    let (tx, mut rx) = channel(cap);

    for i in 0..MESSAGES {
        tx.send(message::new(i)).unwrap();
    }

    for _ in 0..MESSAGES {
        rx.recv().unwrap();
    }
}

fn spsc(cap: usize) {
    let (tx, mut rx) = channel(cap);

    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for i in 0..MESSAGES {
                tx.send(message::new(i)).unwrap();
            }
        });

        for _ in 0..MESSAGES {
            rx.recv().unwrap();
        }
    })
    .unwrap();
}

fn mpsc(cap: usize) {
    let (tx, mut rx) = channel(cap);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..MESSAGES / THREADS {
                    tx.send(message::new(i)).unwrap();
                }
            });
        }

        for _ in 0..MESSAGES {
            rx.recv().unwrap();
        }
    })
    .unwrap();
}

fn main() {
    macro_rules! run {
        ($name:expr, $f:expr) => {
            let now = ::std::time::Instant::now();
            $f;
            let elapsed = now.elapsed();
            println!(
                "{:25} {:15} {:7.3} sec",
                $name,
                "Rust swap-buffer-queue",
                elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9
            );
        };
    }

    run!("bounded1_mpsc", mpsc(1));
    run!("bounded1_spsc", spsc(1));

    run!("bounded_mpsc", mpsc(MESSAGES));
    run!("bounded_seq", seq(MESSAGES));
    run!("bounded_spsc", spsc(MESSAGES));
}
