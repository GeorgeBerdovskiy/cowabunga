//use pyo3::prelude::*;
use rand::prelude::*;

use cowabunga_rs::table::*;
//use cowabunga_rs::bufferpool::*;

use std::collections::HashSet;

fn main() {
    println!("Hello from cowabunga_profile!");


    println!("2 Hello from cowabunga_profile!");

    let tbl = Table::new("./COWA_PROFILE".to_string(), "prof_tbl".to_string(), 8, 0, false);

    println!("3 Hello from cowabunga_profile!");

    let mut prim_keys: HashSet<i64> = HashSet::new();
    let mut rng = rand::thread_rng();

    for i in 0..250_000 {
        match rng.gen_range(0..3) {

            0 => {

                let row: Vec<i64> = (0..8)
                    .map(|_| rng.gen_range(1..=100))
                    .collect();

                prim_keys.insert(row[0]);
                tbl.insert(row);

            },

            1 => {

                let row: Vec<Option<i64>> = (0..8)
                    .map(|_| Some(rng.gen_range(1..=100)))
                    .collect();

                if prim_keys.contains(&row[0].unwrap()) {
                    tbl.update(row[0].unwrap(), row);
                }
            },

            2 => {

                let proj: Vec<usize> = (0..8)
                    .map(|_| rng.gen_range(0..=1))
                    .collect();

               match tbl.select(rng.gen_range(0..8), rng.gen_range(0..8), proj) {
                   Ok(_) => {},
                   Err(_) => {}
               }

            },

            _ => unreachable!(), // This case will never happen
        }

        println!("{}/250k", i);
    }
}

