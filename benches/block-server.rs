#[macro_use]
extern crate criterion;

use criterion::Criterion;
use reqwest;

fn get_block() {
    reqwest::get("http://localhost:8888/bundle?5").unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("get block", |b| b.iter(|| get_block()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
