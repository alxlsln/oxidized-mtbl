use std::env;
use std::fs::File;

use oxidized_mtbl::{CompressionType, WriterBuilder};

fn main() {
    let file = File::create("./test-data2.mtbl".to_owned()).unwrap();

    let mut writer = WriterBuilder::new()
        .compression_type(CompressionType::Snappy)
        .build(file);

    for i in 0..3_000_000 {
        let key = format!("{:010}", i);
        let value = format!("{:010}", i).repeat(i / 10_000);
        writer.insert(key, value).unwrap();
    }

    writer.finish().unwrap();
}
