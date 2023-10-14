use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process;
use std::str;
use std::time::Instant;

use memmap2::Mmap;

fn main() {
    let mut args = env::args_os();
    if args.len() != 3 {
        eprint!(
            "\
Usage: cargo run --release <data> <strategy>

Strategies:
    fulltext
    memmap-ref
    memmap-clone
    read
    read-memmap
"
        );
        process::exit(2);
    }
    _ = args.next();
    let filename = args.next().unwrap();
    let strategy = args.next().unwrap();
    let start = Instant::now();
    match strategy.to_str() {
        Some("fulltext") => calc_key_ref(fs::read(filename).unwrap()),
        Some("memmap-ref") => calc_key_ref(memmap(filename)),
        Some("memmap-clone") => calc_key_clone(memmap(filename)),
        Some("read") => calc_read(File::open(filename).unwrap()),
        Some("read-memmap") => calc_read(&*memmap(filename)),
        _ => panic!("Unknown strategy"),
    }
    println!("Elapsed: {:?}", start.elapsed());
}

#[inline]
fn memmap<P: AsRef<Path>>(path: P) -> Mmap {
    let file = File::open(path).unwrap();
    unsafe { Mmap::map(&file).unwrap() }
}

#[inline]
fn calc_key_ref<T: AsRef<[u8]>>(text: T) {
    let mut lines = text.as_ref().split(|&b| b == b'\n');
    let (idx, header_len) = ColIndices::from_header(lines.next().unwrap());

    let mut products = HashMap::<&[u8], ProductData>::new();
    let mut cols = Vec::with_capacity(header_len);
    for line in lines {
        if line.len() == 0 {
            continue;
        }
        cols.clear();
        cols.extend(line.split(|&b| b == b','));
        if cols[idx.source] == b"ToClnt" {
            let prod = products.entry(cols[idx.prod]).or_default();
            prod.process_row(&cols, &idx);
        }
    }

    let mut stdout = io::stdout().lock();
    for (&prod, data) in &products {
        data.fmt(&mut stdout, prod).unwrap();
    }
}

#[inline]
fn calc_key_clone<T: AsRef<[u8]>>(text: T) {
    let mut lines = text.as_ref().split(|&b| b == b'\n');
    let (idx, header_len) = ColIndices::from_header(lines.next().unwrap());

    let mut products = hashbrown::HashMap::<Box<[u8]>, ProductData>::new();
    let mut cols = Vec::with_capacity(header_len);
    for line in lines {
        if line.len() == 0 {
            continue;
        }
        cols.clear();
        cols.extend(line.split(|&b| b == b','));
        if cols[idx.source] == b"ToClnt" {
            let prod = products.entry_ref(cols[idx.prod]).or_default();
            prod.process_row(&cols, &idx);
        }
    }

    let mut stdout = io::stdout().lock();
    for (prod, data) in &products {
        data.fmt(&mut stdout, prod).unwrap();
    }
}

#[inline]
fn calc_read<R: Read>(reader: R) {
    let mut file = BufReader::new(reader);

    let mut line = Vec::new();
    file.read_until(b'\n', &mut line).unwrap();
    let (idx, header_len) = ColIndices::from_header(&line);

    let mut products = hashbrown::HashMap::<Box<[u8]>, ProductData>::new();
    let mut cols_empty: Vec<&'static [u8]> = Vec::with_capacity(header_len);
    loop {
        line.clear();
        if file.read_until(b'\n', &mut line).unwrap() == 0 {
            break;
        }
        if line.len() == 0 {
            continue;
        }
        let mut cols = cols_empty;
        cols.extend(line.split(|&b| b == b','));
        if cols[idx.source] == b"ToClnt" {
            let prod = products.entry_ref(cols[idx.prod]).or_default();
            prod.process_row(&cols, &idx);
        }
        cols_empty = cols.into_iter().take(0).map(|_| &[][..]).collect();
    }

    let mut stdout = io::stdout().lock();
    for (prod, data) in &products {
        data.fmt(&mut stdout, prod).unwrap();
    }
}

#[derive(Default)]
struct ProductData {
    count: u32,
    buys: u32,
    sells: u32,
    total_qty: u32,
}

struct ColIndices {
    source: usize,
    bs: usize,
    ordqty: usize,
    wrkqty: usize,
    excqty: usize,
    prod: usize,
}

impl ColIndices {
    #[inline]
    fn from_header(header: &[u8]) -> (ColIndices, usize) {
        let mut source_idx = None;
        let mut bs_idx = None;
        let mut ordqty_idx = None;
        let mut wrkqty_idx = None;
        let mut excqty_idx = None;
        let mut prod_idx = None;
        let mut cols = 0;
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
            cols += 1;
        }
        let indices = ColIndices {
            source: source_idx.unwrap(),
            bs: bs_idx.unwrap(),
            ordqty: ordqty_idx.unwrap(),
            wrkqty: wrkqty_idx.unwrap(),
            excqty: excqty_idx.unwrap(),
            prod: prod_idx.unwrap(),
        };
        (indices, cols)
    }
}

impl ProductData {
    #[inline]
    fn process_row(&mut self, cols: &[&[u8]], idx: &ColIndices) {
        #[inline]
        fn parse_u32(s: &[u8]) -> u32 {
            // SAFETY: The grammar for u32::from_str_radix is all ASCII and it
            // parses as bytes, rejecting any non-ASCII sequences, so it handles
            // invalid UTF-8 safely.
            let s = unsafe { str::from_utf8_unchecked(s) };
            s.parse().unwrap()
        }

        self.count += 1;
        match cols[idx.bs] {
            b"Buy" => self.buys += 1,
            b"Sell" => self.sells += 1,
            _ => {}
        }
        let ordqty = parse_u32(cols[idx.ordqty]);
        let wrkqty = parse_u32(cols[idx.wrkqty]);
        let excqty = parse_u32(cols[idx.excqty]);
        self.total_qty += ordqty.max(wrkqty.max(excqty));
    }

    #[inline]
    fn fmt<W: Write>(&self, w: &mut W, prod: &[u8]) -> io::Result<()> {
        w.write_all(prod)?;
        writeln!(
            w,
            " {} buy={} sell={} avg qty={:6.2}",
            self.count,
            self.buys,
            self.sells,
            self.total_qty as f64 / self.count as f64,
        )
    }
}
