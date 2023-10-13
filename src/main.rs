use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::process;
use std::str;

fn main() {
    let args = env::args_os();
    if args.len() != 2 {
        eprintln!("Usage: cargo run <data>");
        process::exit(2);
    }
    let filename = args.skip(1).next().unwrap();
    driver1(filename.as_ref());
}

#[derive(Default)]
struct ProductData {
    count: u32,
    buys: u32,
    sells: u32,
    total_qty: u64,
}

fn driver1(path: &Path) {
    let data = fs::read(path).unwrap();
    let mut lines = data.split(|&b| b == b'\n');

    let header = lines.next().unwrap();
    let mut header_len = 0;
    let mut source_idx = None;
    let mut bs_idx = None;
    let mut ordqty_idx = None;
    let mut wrkqty_idx = None;
    let mut excqty_idx = None;
    let mut prod_idx = None;
    for (i, col) in header.split(|&b| b == b',').enumerate() {
        match col {
            b"Source" => source_idx = Some(i),
            b"B/S" => bs_idx = Some(i),
            b"OrdQty" => ordqty_idx = Some(i),
            b"WrkQty" => wrkqty_idx = Some(i),
            b"ExcQty" => excqty_idx = Some(i),
            b"Prod" => prod_idx = Some(i),
            _ => {}
        }
        header_len += 1;
    }
    let source_idx = source_idx.unwrap();
    let bs_idx = bs_idx.unwrap();
    let ordqty_idx = ordqty_idx.unwrap();
    let wrkqty_idx = wrkqty_idx.unwrap();
    let excqty_idx = excqty_idx.unwrap();
    let prod_idx = prod_idx.unwrap();

    let mut products = HashMap::<&str, ProductData>::new();
    let mut cols = Vec::with_capacity(header_len);
    for line in lines {
        if line.len() == 0 {
            continue;
        }
        cols.clear();
        cols.extend(line.split(|&b| b == b','));
        if cols[source_idx] != b"ToClnt" {
            continue;
        }
        let prod = str::from_utf8(cols[prod_idx]).unwrap();
        let data = products.entry(prod).or_default();
        data.count += 1;
        match cols[bs_idx] {
            b"Buy" => data.buys += 1,
            b"Sell" => data.sells += 1,
            _ => {}
        }
        let ordqty: u64 = str::from_utf8(cols[ordqty_idx]).unwrap().parse().unwrap();
        let wrkqty: u64 = str::from_utf8(cols[wrkqty_idx]).unwrap().parse().unwrap();
        let excqty: u64 = str::from_utf8(cols[excqty_idx]).unwrap().parse().unwrap();
        data.total_qty += ordqty.max(wrkqty.max(excqty));
    }

    let mut stdout = io::stdout().lock();
    for (&prod, data) in &products {
        writeln!(
            stdout,
            "{prod:3} {} buy={} sell={} avg qty={:6.2}",
            data.count,
            data.buys,
            data.sells,
            data.total_qty as f64 / data.count as f64,
        )
        .unwrap();
    }
}
