#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion};
use reqwest;

fn post_block(client: &reqwest::Client, block: u16) {
    client
        .post(&format!("http://localhost:8888/bench/?{}", block % 100 + 1))
        .body(vec![(block + 0x20 % 0x40) as u8; 2048])
        .send()
        .unwrap();
}

fn get_block(client: &reqwest::Client, block: u16) {
    client
        .get(&format!("http://localhost:8888/bench?{}", block % 100 + 1))
        .send()
        .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let client = reqwest::Client::new();
    let mut i = 0u16;
    c.bench_function("post block", move |b| {
        b.iter_batched(
            || {
                i += 1;
                i
            },
            |i| post_block(&client, i),
            BatchSize::SmallInput,
        )
    });
    i = 0;
    let client = reqwest::Client::new();
    c.bench_function("get block", move |b| {
        b.iter_batched(
            || {
                i += 1;
                i
            },
            |i| get_block(&client, i),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
