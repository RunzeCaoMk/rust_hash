use std::time::Instant;
use hash::join::*;
use hash::hash::*;
use rand::{distributions::Alphanumeric, Rng}; // 0.8.5
use std::fs::File;
use std::io::LineWriter;
use std::io::prelude::*;

// function to creat number of tuples for benchmark
pub fn create_vec_tuple(tuple_number: usize, key_length: usize) -> Vec<(Field, Field)> {
    let mut tuples = Vec::new();
    for _ in 0..tuple_number {
        // create a random string as "name" attribute
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(key_length)
            .map(char::from)
            .collect();
        // use fixed "CS" as "department"
        let fields = (Field::StringField(String::from("CS")),
                      Field::StringField(s));
        tuples.push(fields);
    }
    tuples
}

// helper method to benchmark 5k tuples
fn c_5k(mut file: &File) {
    file.write_all("5k:\n".as_ref());
    let mut common = create_vec_tuple((2500 as f64 * 0.1) as usize, 7);
    let mut left_child = create_vec_tuple((2500 as f64 * 0.9) as usize, 7);
    let mut right_child = create_vec_tuple((2500 as f64 * 0.9) as usize, 7);
    left_child.append(&mut common);
    right_child.append(&mut common);
    // // Linear Probe
    // let mut linear_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // linear_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut linear_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // linear_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut linear_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Linear Probe + std Hash:\n".as_ref());
    // let now = Instant::now();
    // linear_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut linear_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // linear_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Hopscotch
    let mut hopscotch_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        50,
        100,
        HashFunction::FarmHash,
        HashScheme::Hopscotch,
        10,
        ExtendOption::ExtendBucketSize,
        0.75,
    );
    file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    let now = Instant::now();
    hopscotch_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut hopscotch_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        50,
        100,
        HashFunction::MurmurHash3,
        HashScheme::Hopscotch,
        10,
        ExtendOption::ExtendBucketSize,
        0.75,
    );
    file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    hopscotch_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut hopscotch_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        50,
        100,
        HashFunction::StdHash,
        HashScheme::Hopscotch,
        10,
        ExtendOption::ExtendBucketSize,
        0.75,
    );
    file.write_all("Hopscotch + std Hash:\n".as_ref());
    let now = Instant::now();
    hopscotch_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut hopscotch_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        50,
        100,
        HashFunction::T1haHash,
        HashScheme::Hopscotch,
        10,
        ExtendOption::ExtendBucketSize,
        0.75,
    );
    file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    hopscotch_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Robin hood
    // let mut RobinHood_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("RobinHood + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut RobinHood_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut RobinHood_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("RobinHood + std Hash:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut RobinHood_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     50,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
}
// helper method to benchmark 100k tuples
fn c_100k(mut file: &File) {
    file.write_all("100k:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark 500k tuples
fn c_500k(mut file: &File) {
    file.write_all("500k:\n".as_ref());
    let left_child = create_vec_tuple(250000, 7);
    let right_child = create_vec_tuple(250000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     5000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     5000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     5000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     5000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        5000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// method to benchmark different cardinality with 12 permutations
fn cardinality(mut file: &File) {
    file.write_all("Micro-benchmark with different cardinality\n".as_ref());
    c_5k(file);
    // c_100k(file);
    // c_500k(file);
}

// helper method to benchmark extend bucket number
fn eo_b_number(mut file: &File) {
    file.write_all("bucket number:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketNumber,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark extend bucket number
fn eo_b_size(mut file: &File) {
    file.write_all("bucket size:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        10,
        500,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.75,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // let mut linear_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::MurmurHash3,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // linear_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut linear_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::StdHash,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Linear Probe + std Hash:\n".as_ref());
    // let now = Instant::now();
    // linear_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut linear_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::T1haHash,
    //     HashScheme::LinearProbe,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // linear_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // // Robin hood
    // let mut RobinHood_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::FarmHash,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("RobinHood + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut RobinHood_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::MurmurHash3,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut RobinHood_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::StdHash,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("RobinHood + std Hash:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut RobinHood_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::T1haHash,
    //     HashScheme::RobinHood,
    //     4,
    //     ExtendOption::ExtendBucketNumber,
    //     0.75,
    // );
    // file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // RobinHood_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
}
// method to benchmark different extend option with 12 permutations
fn extend_option(mut file: &File) {
    file.write_all("Micro-benchmark with different extend option\n".as_ref());
    // eo_b_number(file);
    eo_b_size(file);
}

// helper method to benchmark load factor 0.5
fn lf_05(mut file: &File) {
    file.write_all("0.5:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.5,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.5,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.5,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.5,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.5,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark load factor 0.75
fn lf_07(mut file: &File) {
    file.write_all("0.7:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.7,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.7,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.7,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.7,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.7,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark load factor 1.0
fn lf_10(mut file: &File) {
    file.write_all("1.0:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     1.0,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     1.0,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     1.0,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     1.0,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        1.0,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// method to benchmark different load factor with 12 permutations
fn load_factor(mut file: &File) {
    file.write_all("Micro-benchmark with different load factor\n".as_ref());
    lf_05(file);
    lf_07(file);
    lf_10(file);
}

// helper method to benchmark b_number 500 * b_size 200
fn sn_500_200(mut file: &File) {
    file.write_all("500 buckets * 200 slots/bucket:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     500,
    //     200,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     500,
    //     200,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     500,
    //     200,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     500,
    //     200,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        500,
        200,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark b_number 200 * b_size 500
fn sn_200_500(mut file: &File) {
    file.write_all("200 buckets * 500 slots/bucket:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     200,
    //     500,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        200,
        500,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark b_number 100 * b_size 1000
fn sn_100_1000(mut file: &File) {
    file.write_all("100 buckets * 1000 slots/bucket:\n".as_ref());
    let left_child = create_vec_tuple(50000, 7);
    let right_child = create_vec_tuple(50000, 7);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     100,
    //     1000,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     100,
    //     1000,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     100,
    //     1000,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     100,
    //     1000,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        100,
        1000,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// method to benchmark different b_number and b_size with 12 permutations
fn size_number(mut file: &File) {
    file.write_all("Micro-benchmark with different b_number and b_size\n".as_ref());
    sn_500_200(file);
    sn_200_500(file);
    sn_100_1000(file);
}

// helper method to benchmark key length 20
fn kl_20(mut file: &File) {
    file.write_all("20 key length:\n".as_ref());
    let left_child = create_vec_tuple(50000, 20);
    let right_child = create_vec_tuple(50000, 20);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark key length 100
fn kl_100(mut file: &File) {
    file.write_all("100 key length:\n".as_ref());
    let left_child = create_vec_tuple(50000, 100);
    let right_child = create_vec_tuple(50000, 100);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark key length 500
fn kl_500(mut file: &File) {
    file.write_all("500 key length:\n".as_ref());
    let left_child = create_vec_tuple(50000, 500);
    let right_child = create_vec_tuple(50000, 500);
    // Linear Probe
    let mut linear_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Farm Hash:\n".as_ref());
    let now = Instant::now();
    linear_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    linear_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + std Hash:\n".as_ref());
    let now = Instant::now();
    linear_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut linear_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::LinearProbe,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("Linear Probe + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    linear_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // // Hopscotch
    // let mut hopscotch_farm_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::FarmHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Farm Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_farm_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // let mut hopscotch_murmur_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::MurmurHash3,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + Murmur Hash 3:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_murmur_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_std_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::StdHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + std Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_std_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());
    //
    // let mut hopscotch_t1ha_join = HashEqJoin::new(
    //     left_child.clone(),
    //     right_child.clone(),
    //     1000,
    //     100,
    //     HashFunction::T1haHash,
    //     HashScheme::Hopscotch,
    //     64,
    //     ExtendOption::ExtendBucketSize,
    //     0.9,
    // );
    // file.write_all("Hopscotch + T1ha Hash:\n".as_ref());
    // let now = Instant::now();
    // hopscotch_t1ha_join.join();
    // file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    // file.write_all("\n".as_ref());

    // Robin hood
    let mut RobinHood_farm_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::FarmHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Farm Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_farm_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_murmur_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::MurmurHash3,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + Murmur Hash 3:\n".as_ref());
    let now = Instant::now();
    RobinHood_murmur_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_std_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::StdHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + std Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_std_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    let mut RobinHood_t1ha_join = HashEqJoin::new(
        left_child.clone(),
        right_child.clone(),
        1000,
        100,
        HashFunction::T1haHash,
        HashScheme::RobinHood,
        4,
        ExtendOption::ExtendBucketSize,
        0.9,
    );
    file.write_all("RobinHood + T1ha Hash:\n".as_ref());
    let now = Instant::now();
    RobinHood_t1ha_join.join();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// method to benchmark different key length with 12 permutations
fn key_length(mut file: &File) {
    file.write_all("Micro-benchmark with different key length\n".as_ref());
    kl_20(file);
    kl_100(file);
    kl_500(file);
}

fn main() {
    let mut file = File::create("res2.txt").unwrap();
    cardinality(&file);
    // extend_option(&file);
    // load_factor(&file);
    // size_number(&file);
    // key_length(&file);
}